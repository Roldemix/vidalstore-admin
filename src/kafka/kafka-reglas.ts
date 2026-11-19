// Reglas puras del administrador de Kafka. Viven aparte para poder probarlas
// sin levantar Nest ni un broker.

export const PREFIJO_PROTEGIDO = 'vidalstore.';

export type ClasificacionError =
  | 'no-encontrado'
  | 'conflicto'
  | 'peticion-invalida'
  | 'broker';

// Un grupo "esta corriendo" si tiene miembros o si esta en un rebalanceo.
// Kafka no permite cambiar sus offsets en ese estado.
export function estaCorriendo(estado: string): boolean {
  return (
    estado === 'Stable' ||
    estado === 'PreparingRebalance' ||
    estado === 'CompletingRebalance'
  );
}

// Busca en el mensaje del broker la causa y la traduce a una familia de HTTP.
// El servicio la convierte en 404, 409, 400 o 502; nunca en 500.
export function clasificarErrorKafka(detalle: string): ClasificacionError {
  const texto = detalle.toLowerCase();

  if (texto.includes('unknown_topic') || texto.includes('unknown topic')) {
    return 'no-encontrado';
  }
  if (texto.includes('group_id_not_found') || texto.includes('group id not found')) {
    return 'no-encontrado';
  }
  if (texto.includes('already exists')) {
    return 'conflicto';
  }
  if (texto.includes('non_empty_group') || texto.includes('group is not empty')) {
    return 'conflicto';
  }
  if (
    texto.includes('invalid_partitions') ||
    texto.includes('invalid_replication_factor') ||
    texto.includes('invalid_config') ||
    texto.includes('invalid_request') ||
    texto.includes('policy_violation')
  ) {
    return 'peticion-invalida';
  }
  return 'broker';
}

// Un topic del proyecto no se borra por accidente: hay que repetir su nombre
// exacto en ?confirmar=<nombre>.
export function requiereConfirmacion(nombre: string, confirmar?: string): boolean {
  return nombre.startsWith(PREFIJO_PROTEGIDO) && confirmar !== nombre;
}

// Lag de una particion. Si falta el offset comprometido o el final, no se
// inventa un numero: se devuelve null.
export function calcularLag(
  comprometido: bigint | null,
  final: bigint | null,
): number | null {
  if (comprometido === null || final === null) return null;
  if (comprometido < 0n || final < 0n) return null;
  const diferencia = final - comprometido;
  return diferencia > 0n ? Number(diferencia) : 0;
}

// Un offset comprometido negativo significa "sin compromiso": no se muestra
// como numero porque confundiria con un offset real.
export function offsetVisible(offset: bigint): number | null {
  return offset >= 0n ? Number(offset) : null;
}
