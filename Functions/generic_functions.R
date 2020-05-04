
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
    # se for um site de farmac entao no log guardamos o nome da FARMAC
    if(farmac_name != "" | nchar(farmac_name) > 0 ){
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = ' getFarmacServerCon -> Estabelece uma conexao com o servidor central - Farmac',
                   error =as.character(cond$message) ) 
    } else {
      
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = ' getFarmacServerCon -> Estabelece uma conexao com o servidor central - Farmac',
                   error =as.character(cond$message) ) 
    }
 
  
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
    # se for um site de farmac entao no log guardamos o nome da FARMAC
    if(farmac_name != '' | nchar(farmac_name) > 0 ){
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = ' getLocalServerCon -> Estabelece uma conexao com o PostgreSQL Local',
                   error =as.character(cond$message) ) 
    } else {
      
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = ' getLocalServerCon -> Estabelece uma conexao com o PostgreSQL Local',
                   error =as.character(cond$message) ) 
    }
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
#' data <- Sys.time()
#' action  <- 'get Farmac Patients
#' erro <- 'Can not connect to server + excption.msg '
#' saveLogError(us_name,data,action ,erro )
#' 
saveLogError <- function (us.name, event.date, action, error){
  
  # insere a linha de erro no log
  logErro  <<-  add_row(logErro,us = us.name, data_evento =event.date, accao =action, Erro= error)
  
}



#' sendLogError -> Envia log de erros para o servidor
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df.logdispense o datafrane com os errros de execucao (apenas os novas)  
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- sendLogError(con_postgres,logError)


sendLogError <- function(con_postgres , df.logerror ){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    dbWriteTable(con_postgres, "logerro", df.logerror , row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status <- TRUE
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    if(farmac_name==""){
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = ' sendLogError -> Envia log de erros para o servidor ',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = ' sendLogError -> Envia log de erros para o servidor',
                   error = as.character(cond$message) )  
    }
    
    
    #Choose a return value in case of error
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  return(status)
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
#' data <- Sys.time()
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
#' @param clinic_name nome da us (farmac/us)
#' @param data.evento data em que o erro acontece
#' @param patient paciente 
#' @param dispense.date  data do levantamento na farmac 
#' @return NA
#' @examples 
#' 
#' data <- Sys.time()
#' patient  <- '12/456 - Marta Joao'
#' farmac.name  <- 'Farmac Jardim'
#' dispense.date data  do levantamento
#' saveLogDispensa(farmac.name,data,patient ,dispense.date )
#' 
saveLogDispensa<- function (clinic_name, event.date, patient, dispense.date){
  
  # insere a linha de erro no das dispensas
  log_dispensas  <<-  add_row(log_dispensas,unidade_sanitaria = clinic_name, data_evento = event.date, paciente =patient, data_levantamento= dispense.date)
  
}



#' sendLogDispense -> Envia log de dispensas para o servidor
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df.logdispense o datafrane com as dispensas dos pacientes referidos (apenas as novas)  
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- sendLogDispense(con_farmac,logdispense)
#TODO addicionar bloco de warnings no trycatch

sendLogDispense <- function(con_postgres , df.logdispense ){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    status_send <- dbWriteTable(con_postgres, "logdispense", df.logdispense, row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status_send <- TRUE
    return(status_send)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    if(farmac_name==""){
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = 'sendLogDispense -> Envia log de dispensas do servidor farmac para o servidor Local ',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = 'sendLogDispense -> Envia log de dispensas da farmac para o servidor Farmac',
                   error = as.character(cond$message) )  
    }
    
    
    #Choose a return value in case of error
    return(FALSE)
  }, 
  warning = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # Se for um waring em que nao foi possivel buscar os dados guardar no log e return FALSE
    if(grepl(pattern = 'Could not create execute',x = cond$message,ignore.case = TRUE)){
      
      # guardar o log 
      if(farmac_name==""){
        saveLogError(us.name = main_clinic_name,
                     event.date = as.character(Sys.time()),
                     action = 'sendLogDispense -> Envia log de dispensas do servidor FARMAC  para o servidor local ',
                     error = as.character(cond$message) )  
        
      } else {
        
        saveLogError(us.name = farmac_name,
                     event.date = as.character(Sys.time()),
                     action = 'sendLogDispense -> Envia log de dispensas da farmac para o servidor Farmac',
                     error = as.character(cond$message) )  
      }

      
      return(FALSE)
      
    } else {
      
      if (exists('status_send')){
        
        return(status_send)
      } else {
        return(FALSE)
      }
      
      
    }

  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  return(status)
}






#' getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.farmac  obejcto de conexao com BD iDART
#' @param clinic.name nome da US que referiu os pacientes 
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas de determinada US
#' @examples 
#' clinic_name <- 'CS BAGAMOIO'
#' con_farmac <- getFarmacServerCon()
#' farmac_log_dispense <- getLogDispenseFromServer(con_farmac,clinic_name)
#' 

getLogDispenseFromServer <- function(con.farmac, clinic.name) {
  
  
  log_dispenses <- tryCatch({
    
    
    temp_logs <- dbGetQuery( con.farmac , paste0("SELECT  unidade_sanitaria, data_evento, paciente, data_levantamento FROM public.logdispense where unidade_sanitaria='", clinic.name, "' ;" )  )
    
    return(temp_logs)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US',
                 error = as.character(cond$message) )  
    
    #Choose a return value in case of error
    return(FALSE)
  },
  warning = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US',
                 error = as.character(cond$message) )  
    
    
    # Se for um waring em que nao foi possivel buscar os dados guardar no log e return FALSE
    if(grepl(pattern = 'Could not create execute',x = cond$message,ignore.case = TRUE)){
      
      # guardar o log 
      saveLogError(us.name = clinic.name,
                   event.date = as.character(Sys.time()),
                   action = 'getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US  ',
                   error = as.character(cond$message) )  
      
      return(FALSE)
      
    } else {
      
      if (exists('temp_logs')){
        
        return(temp_logs)
      }
      
      
    }
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  log_dispenses
  
  
}




#' getLogErrorFromServer -> Busca os logs de erro de uma det. US
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.farmac  obejcto de conexao com BD iDART
#' @param clinic.name nome da US que referiu os pacientes 
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas de determinada US
#' @examples 
#' clinic_name <- 'CS BAGAMOIO'
#' con_farmac <- getFarmacServerCon()
#' farmac_log_dispense <- getLogErrorFromServer(con_farmac,clinic_name)
#' 

getLogErrorFromServer <- function(con.farmac, clinic.name) {
  
  
  log_errors <- tryCatch({
    
    
    temp_logs <- dbGetQuery( con.farmac , paste0("SELECT  us, data_evento, accao, erro FROM public.logerro where us = ''", clinic.name, "' ;" )  )
    
    return(temp_logs)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getLogErrorFromServer -> Busca os logs de erro de uma det. US ',
                 error = as.character(cond$message) )  
    
    #Choose a return value in case of error
    return(FALSE)
  },
  warning = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getLogErrorFromServer -> Busca os logs de erro de uma det. US',
                 error = as.character(cond$message) )  
    
    
    # Se for um waring em que nao foi possivel buscar os dados guardar no log e return FALSE
    if(grepl(pattern = 'Could not create execute',x = cond$message,ignore.case = TRUE)){
      
      # guardar o log 
      saveLogError(us.name = clinic.name,
                   event.date = as.character(Sys.time()),
                   action = 'getLogErrorFromServer -> Busca os logs de erro de uma det. US  ',
                   error = as.character(cond$message) )  
      
      return(FALSE)
      
    } else {
      
      if (exists('temp_logs')){
        
        return(temp_logs)
      }
      
      
    }
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  log_dispenses
  
  
}

