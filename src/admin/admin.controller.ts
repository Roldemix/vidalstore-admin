import { Body, Controller, Delete, Get, HttpCode, Param, Post, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import { BindingDto, CrearColaDto, CrearExchangeDto, NombreParamDto } from './dto';
import { RabbitAdminService } from './rabbit-admin.service';

// Las ocho rutas: solo administradores, incluidos los dos GET.
// El controlador valida y delega: no conoce amqplib ni la API del broker.
@Controller('admin')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('administradores')
export class AdminController {
  constructor(private readonly admin: RabbitAdminService) {}

  @Get('colas')
  listarColas() {
    return this.admin.listarColas();
  }

  @Post('colas')
  crearCola(@Body() dto: CrearColaDto) {
    return this.admin.crearCola(dto);
  }

  @Delete('colas/:nombre')
  @HttpCode(204)
  async eliminarCola(@Param() p: NombreParamDto) {
    await this.admin.eliminarCola(p.nombre);
  }

  @Get('exchanges')
  listarExchanges() {
    return this.admin.listarExchanges();
  }

  @Post('exchanges')
  crearExchange(@Body() dto: CrearExchangeDto) {
    return this.admin.crearExchange(dto);
  }

  @Delete('exchanges/:nombre')
  @HttpCode(204)
  async eliminarExchange(@Param() p: NombreParamDto) {
    await this.admin.eliminarExchange(p.nombre);
  }

  @Post('bindings')
  crearBinding(@Body() dto: BindingDto) {
    return this.admin.crearBinding(dto);
  }

  @Delete('bindings')
  @HttpCode(204)
  async eliminarBinding(@Body() dto: BindingDto) {
    await this.admin.eliminarBinding(dto);
  }
}