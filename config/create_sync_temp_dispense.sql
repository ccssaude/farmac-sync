install.packages('tibble')
install.packages('lubridate')
install.packages('dplyr')
install.packages('RMySQL')
install.packages('RPostgreSQL')
install.packages('properties')
install.packages('plyr')
install.packages('stringi')
install.packages('httr')


-- actualizar alteracoesiDART.sql sem apagar as tabelas
-- actualizar os nids na tabela sync_temp_dispense (farmac depois US)

-- alteracao para todas US que fazem referencia de pacientes a farmacs
alter  TABLE public.sync_temp_dispense add column  clinic_name_farmac character varying(255);
alter  TABLE public.sync_temp_dispense add column imported   character varying(255) default null;
ALTER TABLE public.sync_temp_dispense  DROP CONSTRAINT IF EXISTS sync_temp_dispense_pkey;
alter  TABLE public.sync_temp_dispense add column openmrs_status  character varying(255) default null;
alter  TABLE public.sync_temp_dispense add column send_openmrs character varying(255) default null;



-- executar check_sync_temp_dispense_on_packagesR

-- Alteracoes FARMAC SERVER
-- alter  TABLE public.sync_temp_patients add column imported   character varying(255) default null;
-- alter  TABLE public.sync_temp_dispense add column imported   character varying(255) default null;

-- criar user no openmrs  para autenticar as dispensas da farmac authenticate('farmac', 'iD@rt2020!'))
   
  -- Criacao da tabela dos logs No Servidor FARMAC
  DROP TABLE IF EXISTS public.logerro;
  create table public.logerro(
   id  SERIAL PRIMARY KEY,
  us character varying(255) COLLATE pg_catalog."default",
  data_evento character varying(255) COLLATE pg_catalog."default",
  accao  character varying(255) COLLATE pg_catalog."default",
  erro character varying(255) COLLATE pg_catalog."default"
  )
  TABLESPACE pg_default;
  
  DROP TABLE IF EXISTS public.logdispense;
  create table public.logdispense(
   id  SERIAL PRIMARY KEY,
  unidade_sanitaria  character varying(255) COLLATE pg_catalog."default",
  data_evento character  varying(255) COLLATE pg_catalog."default",
  paciente character varying(255) COLLATE pg_catalog."default",
  data_levantamento character varying(255) COLLATE pg_catalog."default"
  )
  TABLESPACE pg_default;
  
  
  -- sync_temp_dispense do servidor farmac
  -- script de criacao da Table: public.sync_temp_dispense
  
  DROP TABLE IF EXISTS public.sync_temp_dispense CASCADE;
  
  CREATE TABLE public.sync_temp_dispense
  (
    
      id integer NOT NULL,
      clinicalstage integer,
      current character(1) COLLATE pg_catalog."default",
      date timestamp with time zone,
      doctor integer,
      duration integer,
      modified character(1) COLLATE pg_catalog."default",
      patient integer NOT NULL,
      sync_temp_dispenseid character varying(255) COLLATE pg_catalog."default",
      weight double precision,
      reasonforupdate character varying(255) COLLATE pg_catalog."default",
      notes character varying(255) COLLATE pg_catalog."default",
      enddate timestamp with time zone,
      drugtypes character varying(20) COLLATE pg_catalog."default",
      regimeid character varying(255) COLLATE pg_catalog."default",
      datainicionoutroservico timestamp(6) with time zone,
      motivomudanca character varying(32) COLLATE pg_catalog."default",
      linhaid character varying(255) COLLATE pg_catalog."default",
      ppe character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      ptv character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      tb character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      tpi character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      tpc character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      dispensatrimestral integer NOT NULL DEFAULT 0,
      tipodt character varying(255) COLLATE pg_catalog."default",
      gaac character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      af character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      ca character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      ccr character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      saaj character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      fr character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
      amountpertime character varying(255) COLLATE pg_catalog."default",
      dispensedate timestamp with time zone,
      drugname character varying(255) COLLATE pg_catalog."default",
      expirydate timestamp with time zone,
      patientid character varying(255) COLLATE pg_catalog."default",
      patientfirstname character varying(255) COLLATE pg_catalog."default",
      patientlastname character varying(255) COLLATE pg_catalog."default",
      dateexpectedstring character varying(255) COLLATE pg_catalog."default",
      pickupdate timestamp with time zone,
      timesperday integer,
      weekssupply integer,
      qtyinhand character varying(255) COLLATE pg_catalog."default",
      summaryqtyinhand character varying(255) COLLATE pg_catalog."default",
      qtyinlastbatch character varying(255) COLLATE pg_catalog."default",
    	clinic_name_farmac character varying(255) COLLATE pg_catalog."default",
    	imported character varying(255) COLLATE pg_catalog."default"  default null
  )
  WITH (
      OIDS = FALSE
  )
  TABLESPACE pg_default;