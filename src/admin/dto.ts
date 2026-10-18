import {
  IsBoolean,
  IsIn,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Matches,
  Max,
  MaxLength,
  Min,
} from 'class-validator';

// Letras, números, punto, guión y guión bajo; sin espacios.
const PATRON_NOMBRE = /^[A-Za-z0-9._-]+$/;
const MSJ_NOMBRE = 'solo admite letras, números, punto, guión y guión bajo (sin espacios)';

export const TIPOS_EXCHANGE = ['direct', 'topic', 'fanout', 'headers'] as const;
export const TIPOS_COLA = ['classic', 'quorum'] as const;

export class NombreParamDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(100)
  @Matches(PATRON_NOMBRE, { message: `nombre ${MSJ_NOMBRE}` })
  nombre!: string;
}

export class CrearColaDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(100)
  @Matches(PATRON_NOMBRE, { message: `nombre ${MSJ_NOMBRE}` })
  nombre!: string;

  // Valor por defecto explícito: true.
  @IsOptional()
  @IsBoolean()
  durable: boolean = true;

  @IsOptional()
  @IsIn(TIPOS_COLA, { message: `tipo debe ser uno de: ${TIPOS_COLA.join(', ')}` })
  tipo?: (typeof TIPOS_COLA)[number];

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  @MaxLength(100)
  @Matches(PATRON_NOMBRE, { message: `deadLetterExchange ${MSJ_NOMBRE}` })
  deadLetterExchange?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  @MaxLength(255)
  deadLetterRoutingKey?: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(1000000)
  maxLength?: number;
}

export class CrearExchangeDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(100)
  @Matches(PATRON_NOMBRE, { message: `nombre ${MSJ_NOMBRE}` })
  nombre!: string;

  @IsIn(TIPOS_EXCHANGE, { message: `tipo debe ser uno de: ${TIPOS_EXCHANGE.join(', ')}` })
  tipo!: (typeof TIPOS_EXCHANGE)[number];

  // Valor por defecto explícito: true.
  @IsOptional()
  @IsBoolean()
  durable: boolean = true;
}

export class BindingDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(100)
  @Matches(PATRON_NOMBRE, { message: `exchange ${MSJ_NOMBRE}` })
  exchange!: string;

  @IsString()
  @IsNotEmpty()
  @MaxLength(100)
  @Matches(PATRON_NOMBRE, { message: `cola ${MSJ_NOMBRE}` })
  cola!: string;

  // 255 bytes es el máximo de una routing key en AMQP. Admite * y # para los topic.
  @IsString()
  @IsNotEmpty()
  @MaxLength(255)
  @Matches(/^\S+$/, { message: 'routingKey no puede tener espacios' })
  routingKey!: string;
}