-- Table: public.moneothing

-- DROP TABLE IF EXISTS public.moneothing;

CREATE TABLE IF NOT EXISTS public.moneothing
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    thingid uuid NOT NULL,
    uniqueidentifier text COLLATE pg_catalog."default" NOT NULL,
    displayname text COLLATE pg_catalog."default",
    CONSTRAINT moneothing_pkey PRIMARY KEY (id),
    CONSTRAINT uq_thing_identifier UNIQUE (thingid, uniqueidentifier)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.moneothing
    OWNER to richie;


    -- Table: public.moneothingrawdata

-- DROP TABLE IF EXISTS public.moneothingrawdata;

CREATE TABLE IF NOT EXISTS public.moneothingrawdata
(
    thingid bigint NOT NULL,
    rawdataid bigint NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    CONSTRAINT fk_moneothing FOREIGN KEY (thingid)
        REFERENCES public.moneothing (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_rawdata FOREIGN KEY (rawdataid)
        REFERENCES public.rawdata (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.moneothingrawdata
    OWNER to richie;

    -- Table: public.rawdata

-- DROP TABLE IF EXISTS public.rawdata;

CREATE TABLE IF NOT EXISTS public.rawdata
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    value text COLLATE pg_catalog."default",
    CONSTRAINT rawdata_pkey PRIMARY KEY (id),
    CONSTRAINT uq_value UNIQUE (value)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.rawdata
    OWNER to richie;

    	ALTER SEQUENCE public.moneothing_id_seq RESTART WITH 1
	ALTER SEQUENCE public.rawdata_id_seq RESTART WITH 1

    CREATE VIEW moneothingwithrawdata AS
    SELECT m.thingid, m.uniqueidentifier, m.displayname, r.value, mr.timestamp
        FROM moneothingrawdata AS mr
		INNER JOIN moneothing AS m ON m.id = mr.thingid
		INNER JOIN rawdata AS r ON r.id = mr.rawdataid

        SELECT pg_size_pretty( pg_database_size('processdata') ); --> 402 MB mit 5000000, 100, 3