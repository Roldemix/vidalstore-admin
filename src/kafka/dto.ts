import {
  ArrayMaxSize,
  IsArray,
  IsBoolean,
  IsInt,
  IsISO8601,
  IsNotEmpty,
  IsObject,
  IsOptional,
  IsString,
  Matches,
  Max,
  MaxLength,
  Min,
} from 'class-validator';

// Nombres de topic y de grupo: letras, numeros, punto, guion y guion bajo.
const PATRON_NOMBRE = /^[A-Za-z0-9._-]+$/;
const MSJ_NOMBRE = 'solo admite letras, numeros, punto, guion y guion bajo (sin espacios)';

export class NombreTopicParamDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(249)
  @Matches(PATRON_NOMBRE, { message: `nombre ${MSJ_NOMBRE}` })
  nombre!: string;
}

export class NombreGrupoParamDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(249)
  @Matches(PATRON_NOMBRE, { message: `grupo ${MSJ_NOMBRE}` })
  id!: string;
}

export class CrearTopicKafkaDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(249)
  @Matches(PATRON_NOMBRE, { message: `nombre ${MSJ_NOMBRE}` })
  nombre!: string;

  @IsInt()
  @Min(1, { message: 'particiones debe ser al menos 1' })
  @Max(1000, { message: 'particiones no puede superar 1000' })
  particiones!: number;

  @IsInt()
  @Min(1, { message: 'replicas debe ser al menos 1' })
  @Max(10, { message: 'replicas no puede superar 10' })
  replicas!: number;

  @IsOptional()
  @IsObject()
  configuraciones?: Record<string, string>;
}

export class CambiarConfigKafkaDto {
  @IsObject()
  configuraciones!: Record<string, string>;
}

export class AumentarParticionesDto {
  @IsInt()
  @Min(1, { message: 'particiones debe ser al menos 1' })
  @Max(1000, { message: 'particiones no puede superar 1000' })
  particiones!: number;
}

export class RebobinarGrupoDto {
  // Exactamente una de las dos opciones; el servicio responde 400 si vienen las dos o ninguna.
  @IsOptional()
  @IsBoolean()
  alPrincipio?: boolean;

  @IsOptional()
  @IsISO8601({}, { message: 'aFecha debe ser una fecha ISO 8601' })
  aFecha?: string;

  // Si se omite, se rebobinan todos los topics del grupo.
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(100)
  @IsString({ each: true })
  @Matches(PATRON_NOMBRE, { each: true, message: `cada tema ${MSJ_NOMBRE}` })
  temas?: string[];
}
