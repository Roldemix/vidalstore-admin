import {
  BadGatewayException,
  BadRequestException,
  ConflictException,
  HttpException,
  Inject,
  Injectable,
  NotFoundException,
  OnModuleDestroy,
} from '@nestjs/common';
import type {
  Admin,
  ConfigDescription,
  Group,
  ListConsumerGroupOffsetsGroup,
} from '@platformatic/kafka';
import {
  AumentarParticionesDto,
  CambiarConfigKafkaDto,
  CrearTopicKafkaDto,
  RebobinarGrupoDto,
} from './dto';
import {
  calcularLag,
  clasificarErrorKafka,
  estaCorriendo,
  offsetVisible,
  requiereConfirmacion,
} from './kafka-reglas';

export const KAFKA_ADMIN = 'KAFKA_ADMIN';

// Valores del protocolo de Kafka. Se declaran aqui para que el servicio no
// dependa en tiempo de ejecucion del paquete (y se pueda probar con un doble).
const RECURSO_TOPIC = 2;
const OPERACION_SET = 0;
const TIMESTAMP_MAS_ANTIGUO = -2n;
const TIMESTAMP_MAS_RECIENTE = -1n;

export interface ParticionVista {
  particion: number;
  offsetComprometido: number | null;
  offsetFinal: number | null;
  lag: number | null;
  replicas: number;
  lider: number;
}

export interface GrupoVista {
  id: string;
  estado: string;
  miembros: number;
  lagTotal: number;
  topics: Array<{ nombre: string; particiones: ParticionVista[] }>;
}

// Toda la administracion de Kafka vive en este servicio. Los controladores
// validan el DTO y delegan; no conocen el cliente Admin ni el protocolo.
@Injectable()
export class KafkaAdminService implements OnModuleDestroy {
  constructor(@Inject(KAFKA_ADMIN) private readonly admin: Admin) {}

  async onModuleDestroy(): Promise<void> {
    await this.admin.close();
  }

  // ---------------------------------------------------------------- topics

  async listarTopics() {
    const nombres = (await this.ejecutar('listar topics', () => this.admin.listTopics()))
      .filter((nombre) => !nombre.startsWith('__'))
      .sort();
    if (nombres.length === 0) return [];

    const metadata = await this.ejecutar('leer metadatos de topics', () =>
      this.admin.metadata({ topics: nombres, forceUpdate: true }),
    );
    const configuraciones = await this.describeConfigs(nombres);

    return nombres.map((nombre) => {
      const meta = metadata.topics.get(nombre);
      const particiones = meta?.partitions ?? [];
      return {
        nombre,
        particiones: particiones.length,
        replicas: particiones[0]?.replicas.length ?? 0,
        detalleParticiones: particiones.map((particion, indice) => ({
          particion: indice,
          lider: particion.leader,
          replicas: particion.replicas,
          isr: particion.isr,
        })),
        configuracion: configuraciones.get(nombre) ?? {},
      };
    });
  }

  async crearTopic(dto: CrearTopicKafkaDto) {
    const existentes = await this.ejecutar('listar topics', () => this.admin.listTopics());
    if (existentes.includes(dto.nombre)) {
      throw new ConflictException(`El topic ${dto.nombre} ya existe`);
    }
    const configuraciones = this.configuracionesComoPares(dto.configuraciones);

    const creados = await this.ejecutar(`crear el topic ${dto.nombre}`, () =>
      this.admin.createTopics({
        topics: [
          {
            topic: dto.nombre,
            partitions: dto.particiones,
            replicas: dto.replicas,
          },
        ],
        configs: configuraciones,
      }),
    );

    const creado = creados[0];
    return {
      nombre: creado?.name ?? dto.nombre,
      particiones: creado?.partitions ?? dto.particiones,
      replicas: creado?.replicas ?? dto.replicas,
      configuracion: creado?.configuration ?? {},
    };
  }

  async cambiarConfiguracion(nombre: string, dto: CambiarConfigKafkaDto) {
    await this.exigirTopic(nombre);
    const pares = this.configuracionesComoPares(dto.configuraciones);
    if (pares.length === 0) {
      throw new BadRequestException('configuraciones no puede venir vacio');
    }

    await this.ejecutar(`cambiar la configuracion de ${nombre}`, () =>
      this.admin.incrementalAlterConfigs({
        validateOnly: false,
        resources: [
          {
            resourceType: RECURSO_TOPIC,
            resourceName: nombre,
            configs: pares.map(({ name, value }) => ({
              name,
              value: String(value ?? ''),
              configOperation: OPERACION_SET,
            })),
          },
        ],
      }),
    );

    const configuracion = (await this.describeConfigs([nombre])).get(nombre) ?? {};
    return { nombre, configuracion };
  }

  async aumentarParticiones(nombre: string, dto: AumentarParticionesDto) {
    const meta = await this.metadataDe(nombre);
    const actuales = meta.partitions.length;
    if (dto.particiones <= actuales) {
      throw new BadRequestException(
        `El topic ${nombre} tiene ${actuales} particiones y se pidieron ${dto.particiones}. ` +
          'Las particiones solo se pueden aumentar: al reducirlas, la key de un evento dejaria de caer ' +
          'en la misma particion y se rompe el orden que el consumidor espera. Kafka tampoco permite ' +
          'reasignar los mensajes ya escritos.',
      );
    }

    await this.ejecutar(`aumentar las particiones de ${nombre}`, () =>
      this.admin.createPartitions({
        topics: [{ name: nombre, count: dto.particiones }],
      }),
    );

    const despues = await this.metadataDe(nombre);
    return {
      nombre,
      particionesAnteriores: actuales,
      particionesActuales: despues.partitions.length,
    };
  }

  async borrarTopic(nombre: string, confirmar?: string) {
    if (requiereConfirmacion(nombre, confirmar)) {
      throw new BadRequestException(
        `El topic ${nombre} esta protegido. Para borrarlo hay que confirmar explicitamente con ` +
          `?confirmar=${nombre}`,
      );
    }
    await this.exigirTopic(nombre);
    await this.ejecutar(`borrar el topic ${nombre}`, () =>
      this.admin.deleteTopics({ topics: [nombre] }),
    );
    return { borrado: nombre };
  }

  // ---------------------------------------------------------------- grupos

  async listarGrupos(): Promise<GrupoVista[]> {
    const grupos = await this.ejecutar('listar grupos', () => this.admin.listGroups());
    const ids = [...grupos.keys()].sort();
    if (ids.length === 0) return [];

    const detalles = await this.ejecutar('describir grupos', () =>
      this.admin.describeGroups({ groups: ids }),
    );
    const offsets = await this.ejecutar('listar offsets de grupos', () =>
      this.admin.listConsumerGroupOffsets({ groups: ids }),
    );
    const offsetsPorGrupo = new Map(offsets.map((grupo) => [grupo.groupId, grupo]));
    const finales = await this.offsetsFinales(offsets);

    return ids.map((id) => {
      const detalle = detalles.get(id);
      const estado = detalle?.state ?? grupos.get(id)?.state ?? 'Unknown';
      const miembros = detalle?.members.size ?? 0;
      const topics = this.armarOffsets(offsetsPorGrupo.get(id), finales);
      const lagTotal = topics.reduce(
        (total, topic) =>
          total + topic.particiones.reduce((suma, p) => suma + (p.lag ?? 0), 0),
        0,
      );
      return { id, estado, miembros, lagTotal, topics };
    });
  }

  async rebobinarGrupo(id: string, dto: RebobinarGrupoDto) {
    const alPrincipio = dto.alPrincipio === true;
    if (alPrincipio === Boolean(dto.aFecha)) {
      throw new BadRequestException(
        'Indica exactamente una opcion: alPrincipio=true o aFecha=<fecha ISO>',
      );
    }

    const detalles = await this.ejecutar(`describir el grupo ${id}`, () =>
      this.admin.describeGroups({ groups: [id] }),
    );
    const detalle = detalles.get(id);
    if (!detalle) throw new NotFoundException(`El grupo ${id} no existe`);

    if (detalle.members.size > 0 || estaCorriendo(detalle.state)) {
      throw new ConflictException(
        `El grupo ${id} esta corriendo (estado ${detalle.state}, ${detalle.members.size} miembros). ` +
          'Deten el servicio que lo consume y vuelve a intentarlo: Kafka solo permite rebobinar un grupo sin miembros.',
      );
    }

    const offsets = await this.ejecutar(`leer los offsets de ${id}`, () =>
      this.admin.listConsumerGroupOffsets({ groups: [id] }),
    );
    const actual = offsets.find((grupo) => grupo.groupId === id);
    if (!actual || actual.topics.length === 0) {
      throw new BadRequestException(`El grupo ${id} no tiene offsets para rebobinar`);
    }

    const temasSolicitados = dto.temas?.length ? new Set(dto.temas) : null;
    const seleccionados = actual.topics.filter(
      (topic) => !temasSolicitados || temasSolicitados.has(topic.name),
    );
    if (temasSolicitados) {
      const faltantes = [...temasSolicitados].filter(
        (tema) => !actual.topics.some((topic) => topic.name === tema),
      );
      if (faltantes.length > 0) {
        throw new BadRequestException(
          `El grupo ${id} no tiene offsets para: ${faltantes.join(', ')}`,
        );
      }
    }
    if (seleccionados.length === 0) {
      throw new BadRequestException(`El grupo ${id} no tiene offsets que coincidan`);
    }

    const fechaMs = dto.aFecha ? Date.parse(dto.aFecha) : null;
    if (dto.aFecha && (fechaMs === null || Number.isNaN(fechaMs))) {
      throw new BadRequestException('aFecha debe ser una fecha ISO valida');
    }

    const destinos = await this.ejecutar(`leer los offsets destino de ${id}`, () =>
      this.admin.listOffsets({
        topics: seleccionados.map((topic) => ({
          name: topic.name,
          partitions: topic.partitions.map((particion) => ({
            partitionIndex: particion.partitionIndex,
            timestamp: alPrincipio ? TIMESTAMP_MAS_ANTIGUO : BigInt(fechaMs as number),
          })),
        })),
      }),
    );

    const destinoPorTema = new Map(
      destinos.map((topic) => [
        topic.name,
        new Map(topic.partitions.map((p) => [p.partitionIndex, p.offset])),
      ]),
    );

    const cambios: Array<{
      nombre: string;
      particiones: Array<{ particion: number; anterior: number; nuevo: number }>;
    }> = [];
    const topicsParaAlterar: Array<{
      name: string;
      partitionOffsets: Array<{ partition: number; offset: bigint }>;
    }> = [];

    for (const topic of seleccionados) {
      const particiones: Array<{ particion: number; anterior: number; nuevo: number }> = [];
      const offsets: Array<{ partition: number; offset: bigint }> = [];

      for (const particion of topic.partitions) {
        const nuevo = destinoPorTema.get(topic.name)?.get(particion.partitionIndex);
        if (nuevo === undefined || nuevo < 0n) continue;
        const anterior = Number(particion.committedOffset);
        if (nuevo === particion.committedOffset) continue;
        offsets.push({ partition: particion.partitionIndex, offset: nuevo });
        particiones.push({ particion: particion.partitionIndex, anterior, nuevo: Number(nuevo) });
      }

      if (offsets.length > 0) {
        topicsParaAlterar.push({ name: topic.name, partitionOffsets: offsets });
        cambios.push({ nombre: topic.name, particiones });
      }
    }

    if (topicsParaAlterar.length === 0) {
      throw new BadRequestException(
        'No hay offsets que cambiar: el grupo ya esta en el punto pedido o la fecha no tiene mensajes',
      );
    }

    await this.ejecutar(`rebobinar el grupo ${id}`, () =>
      this.admin.alterConsumerGroupOffsets({ groupId: id, topics: topicsParaAlterar }),
    );

    return {
      grupo: id,
      momento: alPrincipio ? 'principio' : dto.aFecha,
      topics: cambios,
    };
  }

  // ------------------------------------------------------------- auxiliares

  private async exigirTopic(nombre: string) {
    const topics = await this.ejecutar('listar topics', () => this.admin.listTopics());
    if (!topics.includes(nombre)) {
      throw new NotFoundException(`El topic ${nombre} no existe`);
    }
  }

  private async metadataDe(nombre: string) {
    const metadata = await this.ejecutar(`leer metadatos de ${nombre}`, () =>
      this.admin.metadata({ topics: [nombre], forceUpdate: true }),
    );
    const tema = metadata.topics.get(nombre);
    if (!tema) throw new NotFoundException(`El topic ${nombre} no existe`);
    return tema;
  }

  private async describeConfigs(
    nombres: string[],
  ): Promise<Map<string, Record<string, string | null>>> {
    if (nombres.length === 0) return new Map();
    const respuesta: ConfigDescription[] = await this.ejecutar(
      'leer la configuracion de los topics',
      () =>
        this.admin.describeConfigs({
          includeSynonyms: false,
          resources: nombres.map((nombre) => ({
            resourceType: RECURSO_TOPIC,
            resourceName: nombre,
          })),
        }),
    );

    const mapa = new Map<string, Record<string, string | null>>();
    for (const descripcion of respuesta) {
      const valores: Record<string, string | null> = {};
      for (const config of descripcion.configs) {
        valores[config.name] = config.isSensitive ? null : (config.value ?? null);
      }
      mapa.set(descripcion.resourceName, valores);
    }
    return mapa;
  }

  private configuracionesComoPares(configuraciones?: Record<string, string>) {
    if (!configuraciones) return [];
    return Object.entries(configuraciones).map(([name, value]) => {
      if (typeof value !== 'string') {
        throw new BadRequestException(`La configuracion ${name} debe ser un string`);
      }
      return { name, value };
    });
  }

  private armarOffsets(
    grupo: ListConsumerGroupOffsetsGroup | undefined,
    finales: Map<string, bigint>,
  ): GrupoVista['topics'] {
    if (!grupo) return [];
    return grupo.topics
      .map((topic) => ({
        nombre: topic.name,
        particiones: topic.partitions.map((particion) => {
          const offsetComprometido = offsetVisible(particion.committedOffset);
          const final = finales.get(`${topic.name}:${particion.partitionIndex}`);
          const offsetFinal = final === undefined ? null : offsetVisible(final);
          return {
            particion: particion.partitionIndex,
            offsetComprometido,
            offsetFinal,
            lag: calcularLag(particion.committedOffset, final ?? null),
            replicas: 0,
            lider: -1,
          };
        }),
      }))
      .sort((a, b) => a.nombre.localeCompare(b.nombre));
  }

  private async offsetsFinales(
    grupos: ListConsumerGroupOffsetsGroup[],
  ): Promise<Map<string, bigint>> {
    const porTema = new Map<string, Set<number>>();
    for (const grupo of grupos) {
      for (const topic of grupo.topics) {
        const particiones = porTema.get(topic.name) ?? new Set<number>();
        for (const particion of topic.partitions) particiones.add(particion.partitionIndex);
        porTema.set(topic.name, particiones);
      }
    }
    if (porTema.size === 0) return new Map();

    const respuesta = await this.ejecutar('leer los offsets finales', () =>
      this.admin.listOffsets({
        topics: [...porTema.entries()].map(([name, particiones]) => ({
          name,
          partitions: [...particiones].map((partitionIndex) => ({
            partitionIndex,
            timestamp: TIMESTAMP_MAS_RECIENTE,
          })),
        })),
      }),
    );

    const mapa = new Map<string, bigint>();
    for (const topic of respuesta) {
      for (const particion of topic.partitions) {
        mapa.set(`${topic.name}:${particion.partitionIndex}`, particion.offset);
      }
    }
    return mapa;
  }

  private async ejecutar<T>(que: string, operacion: () => Promise<T>): Promise<T> {
    try {
      return await operacion();
    } catch (error) {
      throw this.traducir(que, error);
    }
  }

  // Nunca se devuelve un 500 por un error del broker: se traduce a 400, 404, 409
  // o 502 con un mensaje que dice que fallo.
  private traducir(que: string, error: unknown): HttpException {
    if (error instanceof HttpException) return error;
    const detalle = error instanceof Error ? error.message : String(error);

    switch (clasificarErrorKafka(detalle)) {
      case 'no-encontrado':
        return new NotFoundException(`Kafka no conoce el recurso al ${que}: ${detalle}`);
      case 'conflicto':
        return new ConflictException(`Kafka rechazo ${que}: ${detalle}`);
      case 'peticion-invalida':
        return new BadRequestException(`Kafka rechazo ${que}: ${detalle}`);
      default:
        return new BadGatewayException(`Kafka no pudo completar ${que}: ${detalle}`);
    }
  }
}
