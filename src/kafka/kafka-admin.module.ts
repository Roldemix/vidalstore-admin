import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Admin } from '@platformatic/kafka';
import { KafkaAdminController } from './kafka-admin.controller';
import { KafkaAdminService, KAFKA_ADMIN } from './kafka-admin.service';

// Un solo Admin por proceso, conectado a los brokers del entorno.
@Module({
  controllers: [KafkaAdminController],
  providers: [
    {
      provide: KAFKA_ADMIN,
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const bootstrapBrokers = (
          config.get<string>('KAFKA_BROKERS') ?? 'localhost:9092'
        )
          .split(',')
          .map((broker) => broker.trim())
          .filter(Boolean);

        return new Admin({
          clientId: 'vidalstore-admin',
          bootstrapBrokers,
          timeout: 10_000,
          retries: 5,
        });
      },
    },
    KafkaAdminService,
  ],
})
export class KafkaAdminModule {}
