import {
  BadGatewayException,
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { BindingDto, CrearColaDto, CrearExchangeDto } from './dto';

type Metodo = 'GET' | 'PUT' | 'POST' | 'DELETE';

interface ColaApi {
  name: string;
  type?: string;
  durable?: boolean;
  messages?: number;
  messages_ready?: number;
  messages_unacknowledged?: number;
  consumers?: number;
  arguments?: Record<string, unknown>;
}
interface ExchangeApi {
  name: string;
  type: string;
  durable: boolean;
}
interface BindingApi {
  routing_key: string;
  properties_key: string;
}

// Toda la administración del broker vive aquí. Los controladores no conocen la API del broker.
// Las escrituras van por la API HTTP de administración (15672): los listados ya tienen que ir
// por HTTP porque por AMQP no se listan colas, así que se usa un solo mecanismo.
@Injectable()
export class RabbitAdminService {
  private readonly base: string;
  private readonly auth: string;
  private readonly vhost: string;

  constructor(config: ConfigService) {
    this.base = config.getOrThrow<string>('RABBIT_API_URL').replace(/\/$/, '');
    const usuario = config.getOrThrow<string>('RABBIT_USER');
    const clave = config.getOrThrow<string>('RABBIT_PASSWORD');
    this.auth = 'Basic ' + Buffer.from(`${usuario}:${clave}`).toString('base64');
    this.vhost = encodeURIComponent(config.get<string>('RABBIT_VHOST') ?? '/');
  }

  // ---- colas ----
  async listarColas() {
    const colas = (await this.llamar<ColaApi[]>('GET', `/queues/${this.vhost}`)) ?? [];
    return colas.map((q) => ({
      nombre: q.name,
      tipo: q.type ?? (q.arguments?.['x-queue-type'] as string | undefined) ?? 'classic',
      durable: q.durable ?? false,
      mensajes: q.messages ?? 0,
      mensajesListos: q.messages_ready ?? 0,
      mensajesUnacked: q.messages_unacknowledged ?? 0,
      consumidores: q.consumers ?? 0,
    }));
  }

  async crearCola(dto: CrearColaDto) {
    const tipo = dto.tipo ?? 'classic';
    const durable = dto.durable ?? true;
    if (tipo === 'quorum' && !durable) {
      throw new BadRequestException('durable: una cola quorum debe ser durable');
    }
    if (dto.deadLetterRoutingKey && !dto.deadLetterExchange) {
      throw new BadRequestException('deadLetterRoutingKey: requiere deadLetterExchange');
    }
    const argumentos: Record<string, unknown> = { 'x-queue-type': tipo };
    if (dto.deadLetterExchange) argumentos['x-dead-letter-exchange'] = dto.deadLetterExchange;
    if (dto.deadLetterRoutingKey) argumentos['x-dead-letter-routing-key'] = dto.deadLetterRoutingKey;
    if (dto.maxLength !== undefined) argumentos['x-max-length'] = dto.maxLength;

    await this.llamar('PUT', `/queues/${this.vhost}/${encodeURIComponent(dto.nombre)}`, {
      durable,
      auto_delete: false,
      arguments: argumentos,
    });
    return { nombre: dto.nombre, tipo, durable, argumentos };
  }

  async eliminarCola(nombre: string) {
    await this.llamar('DELETE', `/queues/${this.vhost}/${encodeURIComponent(nombre)}`);
  }

  // ---- exchanges ----
  async listarExchanges() {
    const exchanges =
      (await this.llamar<ExchangeApi[]>('GET', `/exchanges/${this.vhost}`)) ?? [];
    return exchanges.map((e) => ({
      nombre: e.name || '(default)',
      tipo: e.type,
      durable: e.durable,
    }));
  }

  async crearExchange(dto: CrearExchangeDto) {
    const durable = dto.durable ?? true;
    await this.llamar('PUT', `/exchanges/${this.vhost}/${encodeURIComponent(dto.nombre)}`, {
      type: dto.tipo,
      durable,
      auto_delete: false,
      internal: false,
      arguments: {},
    });
    return { nombre: dto.nombre, tipo: dto.tipo, durable };
  }

  async eliminarExchange(nombre: string) {
    await this.llamar('DELETE', `/exchanges/${this.vhost}/${encodeURIComponent(nombre)}`);
  }

  // ---- bindings ----
  async crearBinding(dto: BindingDto) {
    await this.llamar('POST', this.rutaBinding(dto), { routing_key: dto.routingKey });
    return { exchange: dto.exchange, cola: dto.cola, routingKey: dto.routingKey };
  }

  async eliminarBinding(dto: BindingDto) {
    const ruta = this.rutaBinding(dto);
    const existentes = (await this.llamar<BindingApi[]>('GET', ruta)) ?? [];
    const binding = existentes.find((b) => b.routing_key === dto.routingKey);
    if (!binding) {
      throw new NotFoundException('No existe un binding con ese exchange, cola y routingKey');
    }
    // properties_key ya viene escapado por el broker: se usa tal cual (soporta # y *).
    await this.llamar('DELETE', `${ruta}/${binding.properties_key}`);
  }

  private rutaBinding(dto: BindingDto): string {
    return (
      `/bindings/${this.vhost}/e/${encodeURIComponent(dto.exchange)}` +
      `/q/${encodeURIComponent(dto.cola)}`
    );
  }

  // ---- llamada a la API HTTP del broker ----
  private async llamar<T = unknown>(
    metodo: Metodo,
    ruta: string,
    cuerpo?: unknown,
  ): Promise<T | null> {
    let res: Response;
    try {
      res = await fetch(`${this.base}/api${ruta}`, {
        method: metodo,
        headers: {
          Authorization: this.auth,
          ...(cuerpo !== undefined ? { 'Content-Type': 'application/json' } : {}),
        },
        body: cuerpo !== undefined ? JSON.stringify(cuerpo) : undefined,
        signal: AbortSignal.timeout(8000),
      });
    } catch (err) {
      const motivo = err instanceof Error ? err.message : String(err);
      throw new BadGatewayException(`No se pudo contactar la API del broker: ${motivo}`);
    }
    const texto = await res.text();
    let datos: unknown = null;
    if (texto) {
      try {
        datos = JSON.parse(texto);
      } catch {
        datos = texto;
      }
    }
    if (res.ok) return datos as T | null;

    const razon =
      datos && typeof datos === 'object' && 'reason' in datos
        ? String((datos as { reason: unknown }).reason)
        : res.statusText;
    if (res.status === 404) throw new NotFoundException(razon || 'No existe en el broker');
    if (res.status === 401 || res.status === 403) {
      throw new BadGatewayException('El broker rechazó las credenciales de administración');
    }
    if (res.status >= 400 && res.status < 500) {
      throw new BadRequestException(`El broker rechazó la operación: ${razon}`);
    }
    throw new BadGatewayException(`Error del broker (${res.status}): ${razon}`);
  }
}