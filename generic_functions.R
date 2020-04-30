
#' getFarmacServerCon -> Estabelece uma conexao com o servidor central - Farmac
#' 
#' @param postgres.user username do postgres
#' @param postgres.password passwd
#' @param postgres.db.name nome da db
#' @param postgres.host IP do servidor provincial (mail.ccsaude.org.mz)
#' @return con/FALSE  (con) - retorna um conexao valida  (FALSE) - erro de conexao   
#' @examples 
#' con_farmac <- getFarmacServerCon()
#' 
getFarmacServerCon <- function(){
  
  # Carrega os parametros de conexao
  source('config/config_farmac_server_properties.R')
  
  status <- tryCatch({
    
    
    # imprimme uma msg na consola
    message(paste0( "Postgres - conectando-se ao servidor FARMAC : ",
                    farmac.postgres.host, ' - db:',farmac.postgres.db.name, "...") )
    

    
    # Objecto de connexao com a bd openmrs postgreSQL
    con_postgres <-  dbConnect(PostgreSQL(),user = farmac.postgres.user,
                               password = farmac.postgres.password, 
                               dbname = farmac.postgres.db.name,
                               host = farmac.postgres.host,
                               port = farmac.postgres.port )
    
    return(con_postgres)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    
    # imprimir msg na consola
    
    message(paste0( "PosgreSQL - Nao foi possivel connectar-se ao host: ",
                    farmac.postgres.host, '  db:',farmac.postgres.db.name,
                    "...",'user:',farmac.postgres.user,
                    ' passwd: ', farmac.postgres.password)) 
    message(cond)
    
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.Date()),
                 action = ' getFarmacServerCon -> Estabelece uma conexao com o servidor central - Farmac',
                 error =as.character(cond$message) )  
  
    #Choose a return value in case of error
    return(FALSE)
  },
  warning = function(cond) {
    message("Warning.... Here's the original warning message:")
    # Choose a return value in case of warning
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.Date()),
                 action = 'Warning... getFarmacServerCon -> Estabelece uma conexao com o servidor central - Farmac',
                 error = as.character(cond$message) )  
    
    return(con_postgres)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
}



#' getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local
#' 
#' @param postgres.user username do postgres
#' @param postgres.password passwd
#' @param postgres.db.name nome da db no postgres
#' @param postgres.host localhost
#' @return FALSE/Con -  (con) retorna um conexao valida  (FALSE) - erro de conexao   
#' @examples 
#' con_local<- getLocalServerCon()
#' 
getLocalServerCon <- function(){
  
  
  # Carrega os parametros de conexao
  source('config/config_local_server_properties.R')
  
  status <- tryCatch({
    
    
    # imprimme uma msg na consola
    message(paste0( "Postgres - conectando-se ao servidor FARMAC : ",
                    local.postgres.host, ' - db:',local.postgres.db.name, "...") )
    
    
    
    # Objecto de connexao com a bd openmrs postgreSQL
    con_postgres <-  dbConnect(PostgreSQL(),user = local.postgres.user,
                               password = local.postgres.password, 
                               dbname = local.postgres.db.name,
                               host = local.postgres.host,
                               port = local.postgres.port )
    
    return(con_postgres)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    
    # imprimir msg na consola
    
    message(paste0( "PosgreSQL - Nao foi possivel connectar-se ao host: ",
                    local.postgres.host, '  db:',local.postgres.db.name,
                    "...",'user:',local.postgres.user,
                    ' passwd: ', local.postgres.password)) 
    message(cond)
    
    
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.Date()),
                 action = ' getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local',
                 error =as.character(cond$message) )  
    #Choose a return value in case of error
    return(FALSE)
  },
  warning = function(cond) {
    message("Warning.... Here's the original warning message:")
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.Date()),
                 action = 'Warning getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local',
                 error =as.character(cond$message) )  
    # Choose a return value in case of warning
    return(con_postgres)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
 
}



#' getFarmacSyncTempPatients -> Busca pacientes de uma det. US na tabela sync_temp_patient 
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.postgres  obejcto de conexao com BD iDART
#' @param main.clinic.name nome da us 
#' @return tabela/dataframe/df com todos pacientes da tabela sync_temp_pacientes de determinada US
#' @examples 
#' main.clinic.name <- 'CS Albazine'
#' con_farmac <- getFarmacServerCon()
#' farmac_temp_sync_patients <- getFarmacSyncTempPatients(con_farmac,main.clinic.name)
#' 

getFarmacSyncTempPatients <- function(con.farmac, main.clinic.name) {
  
  
  farmac_sync_temp_patients  <- dbGetQuery( con.farmac , paste0("select * from public.sync_temp_patients  where mainclinicname ='", main.clinic.name, "' ;" )  )
  
  return(farmac_sync_temp_patients)
  
}

#' getLocalSyncTempPatients -> Busca pacientes de uma det. US  na tabela sync_temp_patient 
#' no servidor Local PosgreSQL 
#' 
#' @param con.postgres  obejcto de conexao com BD iDART
#' @param main.clinic.name nome da us 
#' @return tabela/dataframe/df com todos pacientes da tabela sync_temp_pacientes de determinada US
#' @examples 
#' main.clinic.name <- 'CS Albazine'
#' con_local <- getLocalServerCon()
#' sync_patients_farmac <- getLocalSyncTempPatients(con_farmac,main.clinic.name)
#' 

getLocalSyncTempPatients <- function(con.local, main.clinic.name) {
  
  
  sync_temp_patients  <- dbGetQuery( con.local , paste0("select * from public.sync_temp_patients  where mainclinicname ='", main.clinic.name, "' ;" )  )
  
  return(sync_temp_patients)
  
}



#' refferPatients -> Envia pacientes referidos para o Servidor Farmac 
#' 
#' @param con.farmac  obejcto de conexao com BD
#' @param reffered.patients o datafrane com os pacientes referidos (apenas os novos)  
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- refferPatients(con_farmac,pacientes_referidos)

refferPatients<- function(con.farmac, reffered.patients) {
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status= 
  return(status)
  
  status <- tryCatch({
    
    dbWriteTable(con_farmac, "sync_temp_patients", reffered.patients, row.names=FALSE, append=TRUE)
    
    return(TRUE)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(paste0( "PosgreSQL - Nao foi possivel actualizar os pacientes -se ao host: ",
                    farmac.postgres.host, '  db:',farmac.postgres.db.name,
                    "...",'user:',farmac.postgres.user,
                    ' passwd: ', farmac.postgres.password)) 
    message(cond)
    
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.Date()),
                 action = ' refferPatients -> Envia pacientes referidos para o Servidor Farmac',
                 error = as.character(cond$message) )  
    
    #Choose a return value in case of error
    return(FALSE)
  },
  warning = function(cond) {
    message("Warning.... Here's the original warning message:")
    # Choose a return value in case of warning
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.Date()),
                 action = 'Warning refferPatients -> Envia pacientes referidos para o Servidor Farmac',
                 error = as.character(cond$message) )  
    return(TRUE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
}


#' saveLogError -> guardar no log de erros  qualquer erro  que decorre durante a execucao
#' de um procedimento
#' 
#' @param us.name nome US
#' @param data.evento data em que o erro acontece
#' @param accao o que estava a tentar-se executar
#' @param erro msg de erro  
#' @return NA
#' @examples 
#' 
#' us_name <- 'CS Albazine'
#' data <- Sys.Date()
#' action  <- 'get Farmac Patients
#' erro <- 'Can not connect to server + excption.msg '
#' saveLogError(us_name,data,action ,erro )
#' 
saveLogError <- function (us.name, event.date, action, error){
  
  # insere a linha de erro no log
  logErro  <<-  add_row(logErro,us = us.name, data_evento =event.date, accao =action, Erro= error)
  
}



#' saveLogReferencia -> guardar informacao da referencia no log de pacientes referidos 
#' 
#' @param us.name nome US
#' @param data.evento data em que o erro acontece
#' @param patient paciente referido
#' @param us.ref us para onde foi referenciado referencia  
#' @return NA
#' @examples 
#' 
#' us_name <- 'CS Albazine'
#' data <- Sys.Date()
#' paticoen  <- '12/456 - Marta Joao'
#' us_ref  <- 'Farmac Jardim'
#' saveLogReferencia(us_name,data,patient ,us_ref )
#' 
saveLogReferencia<- function (us.name, event.date, patient, us.ref){
  
  # insere a linha de erro no log
  logReferencia  <<-  add_row(logReferencia,unidade_sanitaria = us.name, data_evento =event.date, paciente =patient, referido_para= us.ref)
  
}
