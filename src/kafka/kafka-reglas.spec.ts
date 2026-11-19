import {
  calcularLag,
  clasificarErrorKafka,
  estaCorriendo,
  offsetVisible,
  requiereConfirmacion,
} from './kafka-reglas';

describe('estaCorriendo', () => {
  it('considera corriendo un grupo estable o en rebalanceo', () => {
    expect(estaCorriendo('Stable')).toBe(true);
    expect(estaCorriendo('PreparingRebalance')).toBe(true);
    expect(estaCorriendo('CompletingRebalance')).toBe(true);
  });

  it('permite rebobinar un grupo vacio o muerto', () => {
    expect(estaCorriendo('Empty')).toBe(false);
    expect(estaCorriendo('Dead')).toBe(false);
  });
});

describe('requiereConfirmacion', () => {
  it('protege los topics del proyecto salvo confirmacion exacta', () => {
    expect(requiereConfirmacion('vidalstore.compras')).toBe(true);
    expect(requiereConfirmacion('vidalstore.compras', 'otro')).toBe(true);
    expect(requiereConfirmacion('vidalstore.compras', 'vidalstore.compras')).toBe(false);
  });

  it('no exige confirmacion para topics ajenos al proyecto', () => {
    expect(requiereConfirmacion('pruebas.temporales', 'cualquiera')).toBe(false);
  });
});

describe('calcularLag', () => {
  it('resta el offset comprometido al final', () => {
    expect(calcularLag(10n, 25n)).toBe(15);
  });

  it('nunca devuelve lag negativo', () => {
    expect(calcularLag(30n, 20n)).toBe(0);
  });

  it('devuelve null si falta un offset o si el comprometido es -1', () => {
    expect(calcularLag(null, 20n)).toBeNull();
    expect(calcularLag(10n, null)).toBeNull();
    expect(calcularLag(-1n, 20n)).toBeNull();
  });
});

describe('offsetVisible', () => {
  it('oculta el -1 que significa sin compromiso', () => {
    expect(offsetVisible(-1n)).toBeNull();
    expect(offsetVisible(0n)).toBe(0);
    expect(offsetVisible(42n)).toBe(42);
  });
});

describe('clasificarErrorKafka', () => {
  it('reconoce un topic desconocido', () => {
    expect(clasificarErrorKafka('UNKNOWN_TOPIC_OR_PARTITION: no existe')).toBe('no-encontrado');
  });

  it('reconoce un grupo desconocido', () => {
    expect(clasificarErrorKafka('GROUP_ID_NOT_FOUND')).toBe('no-encontrado');
  });

  it('reconoce un topic duplicado como conflicto', () => {
    expect(clasificarErrorKafka('Topic already exists')).toBe('conflicto');
  });

  it('reconoce un grupo con miembros como conflicto', () => {
    expect(clasificarErrorKafka('NON_EMPTY_GROUP')).toBe('conflicto');
  });

  it('reconoce una peticion invalida', () => {
    expect(clasificarErrorKafka('INVALID_PARTITIONS: no se puede reducir')).toBe(
      'peticion-invalida',
    );
    expect(clasificarErrorKafka('POLICY_VIOLATION')).toBe('peticion-invalida');
  });

  it('todo lo demas es una falla del broker', () => {
    expect(clasificarErrorKafka('request timed out')).toBe('broker');
  });
});
