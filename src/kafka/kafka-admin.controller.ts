import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  Param,
  Patch,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import {
  AumentarParticionesDto,
  CambiarConfigKafkaDto,
  CrearTopicKafkaDto,
  NombreGrupoParamDto,
  NombreTopicParamDto,
  RebobinarGrupoDto,
} from './dto';
import { KafkaAdminService } from './kafka-admin.service';

// Las siete rutas de IE3 e IE9. Solo administradores, igual que el admin de Rabbit.
// El controlador valida y delega: no conoce el cliente Admin de Kafka.
@Controller('admin/kafka')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('administradores')
export class KafkaAdminController {
  constructor(private readonly kafka: KafkaAdminService) {}

  @Get('topics')
  listarTopics() {
    return this.kafka.listarTopics();
  }

  @Post('topics')
  crearTopic(@Body() dto: CrearTopicKafkaDto) {
    return this.kafka.crearTopic(dto);
  }

  @Patch('topics/:nombre/config')
  cambiarConfiguracion(
    @Param() params: NombreTopicParamDto,
    @Body() dto: CambiarConfigKafkaDto,
  ) {
    return this.kafka.cambiarConfiguracion(params.nombre, dto);
  }

  @Post('topics/:nombre/particiones')
  aumentarParticiones(
    @Param() params: NombreTopicParamDto,
    @Body() dto: AumentarParticionesDto,
  ) {
    return this.kafka.aumentarParticiones(params.nombre, dto);
  }

  @Delete('topics/:nombre')
  borrarTopic(
    @Param() params: NombreTopicParamDto,
    @Query('confirmar') confirmar?: string,
  ) {
    return this.kafka.borrarTopic(params.nombre, confirmar);
  }

  @Get('grupos')
  listarGrupos() {
    return this.kafka.listarGrupos();
  }

  @Post('grupos/:id/offsets')
  @HttpCode(200)
  rebobinarGrupo(@Param() params: NombreGrupoParamDto, @Body() dto: RebobinarGrupoDto) {
    return this.kafka.rebobinarGrupo(params.id, dto);
  }
}
