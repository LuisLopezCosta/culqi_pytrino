drop table if exists dl_sandbox.edm_pcom.marimar;

create table dl_sandbox.edm_pcom.marimar
(
	nombre varchar(100),
	apellido varchar(100),
	edad int,
	tiempo_dias double,
	fecha_creacion timestamp(6),
	fecha_nacimiento date,
	fecha_actualizacion timestamp(6)
);
