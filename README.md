# vidalstore-admin

API de administración de VidalStore. Expone la administración de RabbitMQ y de
Kafka, siempre detrás de Cognito: `JwtAuthGuard` valida el token y `RolesGuard`
exige el grupo `administradores`.

## Variables

| Variable | Uso |
|---|---|
| `PORT` | puerto HTTP (3020 por defecto) |
| `KAFKA_BROKERS` | brokers para el cliente `Admin` |
| `RABBIT_API_URL`, `RABBIT_USER`, `RABBIT_PASSWORD`, `RABBIT_VHOST` | API de RabbitMQ |
| `COGNITO_USER_POOL_ID`, `COGNITO_CLIENT_IDS` | validación del token |

## Kafka (EP3)

Todas las rutas cuelgan de `/v1/admin/kafka` y delegan en `KafkaAdminService`,
el único lugar que conoce `@platformatic/kafka`.

| Método | Ruta | Qué hace |
|---|---|---|
| `GET` | `/topics` | lista topics con particiones, réplicas y configuración |
| `POST` | `/topics` | crea un topic (`nombre`, `particiones`, `replicas`, `configuraciones`) |
| `PATCH` | `/topics/:nombre/config` | cambia configuración con `incrementalAlterConfigs` |
| `POST` | `/topics/:nombre/particiones` | aumenta particiones (nunca las reduce) |
| `DELETE` | `/topics/:nombre?confirmar=<nombre>` | borra un topic; los `vidalstore.*` exigen confirmación |
| `GET` | `/grupos` | grupos con estado, miembros, offset comprometido y lag |
| `POST` | `/grupos/:id/offsets` | rebobina un grupo detenido (`alPrincipio` o `aFecha`) |

Reglas de la entrega:

- Rebobinar un grupo con miembros o en rebalanceo responde **409**, no 500.
- Pedir menos particiones responde **400** explicando que se rompería el orden
  por key y que Kafka no reasigna lo ya escrito.
- Los errores del broker se traducen a 400/404/409/502 con el motivo.
- Un topic `vidalstore.*` solo se borra repitiendo su nombre en `?confirmar=`.

## Pruebas

```bash
npm ci
npm run build
npm test
```

`src/kafka/kafka-reglas.spec.ts` cubre las reglas puras (protección de borrado,
lag, traducción de errores, grupo corriendo) sin necesitar un broker.
