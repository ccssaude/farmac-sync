
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
  source('config/config_properties.R')
  
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
    
    ## Coisas a fazer se ocorrer um erro 
    
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
  source('config/config_properties.R')
  
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
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  status
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


#' saveLogDispensa -> guardar informacao de dispensas  no log de dispensas  
#' 
#' @param us.name farnac_name
#' @param data.evento data em que o erro acontece
#' @param patient paciente 
#' @param dispense.date  data do levantamento na farmac 
#' @return NA
#' @examples 
#' 
#' data <- Sys.Date()
#' patient  <- '12/456 - Marta Joao'
#' farmac.name  <- 'Farmac Jardim'
#' dispense.date data  do levantamento
#' saveLogDispensa(us_name,data,patient ,us_ref )
#' 
saveLogDispensa<- function (farmac.name, event.date, patient, dispense.date){
  
  # insere a linha de erro no das dispensas
  log_dispensas  <<-  add_row(log_dispensas,unidade_sanitaria = farmac.name, data_evento =event.date, paciente =patient, data_levantamento= dispense.date)
  
}

