import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AdminController } from './admin/admin.controller';
import { RabbitAdminService } from './admin/rabbit-admin.service';
import { KafkaAdminModule } from './kafka/kafka-admin.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), KafkaAdminModule],
  controllers: [AdminController],
  providers: [RabbitAdminService],
})
export class AppModule {}
