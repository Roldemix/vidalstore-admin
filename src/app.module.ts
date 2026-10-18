import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AdminController } from './admin/admin.controller';
import { RabbitAdminService } from './admin/rabbit-admin.service';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true })],
  controllers: [AdminController],
  providers: [RabbitAdminService],
})
export class AppModule {}