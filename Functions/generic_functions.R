library(dplyr)
library(plyr)
library(stringi)
library(tibble)
library(properties)
library(httr)
library(stringr)
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
    if(is.farmac){
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
    message(paste0( "Postgres - conectando-se ao servidor Local : ",
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
    # guardar o log 
    # if(is.farmac){
    #   saveLogError(us.name = farmac_name,
    #                event.date = as.character(Sys.time()),
    #                action = 'getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local ',
    #                error = as.character(cond$message) )  
    #   
    # } else {
    #   
    #   saveLogError(us.name = main_clinic_name ,
    #                event.date = as.character(Sys.time()),
    #                action = 'getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local',
    #                error = as.character(cond$message) )  
    #}
    
    
 
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
  logErro  <<-  add_row(logErro,us = us.name, data_evento =event.date, accao =action, erro= error)
  
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
    if(is.farmac){
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = ' sendLogError -> Envia log de erros para o servidor ',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = main_clinic_name ,
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
    if(is.farmac){
      saveLogError(us.name = farmac_name ,
                   event.date = as.character(Sys.time()),
                   action = 'sendLogDispense -> Envia log de dispensas do servidor farmac para o servidor Local ',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = main_clinic_name,
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
      if(fis.farmac){
        saveLogError(us.name = farmac_name  ,
                     event.date = as.character(Sys.time()),
                     action = 'sendLogDispense -> Envia log de dispensas do servidor FARMAC  para o servidor local ',
                     error = as.character(cond$message) )  
        
      } else {
        
        saveLogError(us.name = main_clinic_name,
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



#' savePackageDrugInfoTmp -> insere um registo na tabela savePackageDrugInfoTmp
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df o datafrane com o mesmo formato da tabela savePackageDrugInfoTmp
#' @param table.name o nome da tabela na BD
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- savePackageDrugInfoTmp(con_farmac,df.packagedruginfotmp)


savePackageDrugInfoTmp <- function(con_postgres , df.packagedruginfotmp){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    status_send <- dbWriteTable( con_postgres, "packagedruginfotmp", df.packagedruginfotmp, row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status_send <- TRUE
    return(status_send)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    if(is.farmac){
      saveLogError(us.name = farmac_name ,
                   event.date = as.character(Sys.time()),
                   action = paste0( "  savePackageDrugInfoTmp -> insere um registo na tabela savePackageDrugInfoTmp"),
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = main_clinic_name ,
                   event.date = as.character(Sys.time()),
                   action = paste0( "  savePackageDrugInfoTmp -> insere um registo na tabela savePackageDrugInfoTmp"),
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
      if(is.farmac){
        saveLogError(us.name =farmac_name ,
                     event.date = as.character(Sys.time()),
                     action = paste0( "  savePackageDrugInfoTmp -> insere um registo na tabela savePackageDrugInfoTmp"),
                     error = as.character(cond$message) )  
        
      } else {
        
        saveLogError(us.name =main_clinic_name  ,
                     event.date = as.character(Sys.time()),
                     action = paste0( "  savePackageDrugInfoTmp -> insere um registo na tabela savePackageDrugInfoTmp"),
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


#' savePackage -> insere um registo numa tabela Package
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df.package o datafrane com o mesmo formato da tabelaPackage
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- savePackage(con_farmac, df.package)


savePackage <- function(con_postgres , df.package){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    status_send <- dbWriteTable(con_postgres,"package", df.package, row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status_send <- TRUE
    return(status_send)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    if(is.farmac){
      saveLogError(us.name = farmac_name ,
                   event.date = as.character(Sys.time()),
                   action = paste0( " savePackage -> insere um registo numa tabela Package"),
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = main_clinic_name ,
                   event.date = as.character(Sys.time()),
                   action = paste0( " savePackage -> insere um registo numa tabela Package"),
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
      if(is.farmac){
        saveLogError(us.name =farmac_name ,
                     event.date = as.character(Sys.time()),
                     action = paste0( " savePackage -> insere um registo numa tabela Package"),
                     error = as.character(cond$message) )  
        
      } else {
        
        saveLogError(us.name =main_clinic_name  ,
                     event.date = as.character(Sys.time()),
                     action = paste0( " savePackage -> insere um registo numa tabela Package"),
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





#' savePackagedDrugs -> insere um registo numa tabela PackagedDrug
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df.packageddrugs o datafrane com o mesmo formato da tabela PackagedDrug
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- savePackagedDrugs(con_farmac,df.packageddrugs)


savePackagedDrugs <- function(con_postgres , df.packageddrugs){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    status_send <- dbWriteTable(con_postgres,"packageddrugs", df.packageddrugs, row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status_send <- TRUE
    return(status_send)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    if(is.farmac){
      saveLogError(us.name = farmac_name ,
                   event.date = as.character(Sys.time()),
                   action = paste0( " savePackagedDrugs -> insere um registo numa tabela Package"),
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = main_clinic_name ,
                   event.date = as.character(Sys.time()),
                   action = paste0( " savePackagedDrugs -> insere um registo numa tabela Package"),
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
      if(is.farmac){
        saveLogError(us.name =farmac_name ,
                     event.date = as.character(Sys.time()),
                     action = paste0( " savePackagedDrugs -> insere um registo numa tabela Package"),
                     error = as.character(cond$message) )  
        
      } else {
        
        saveLogError(us.name =main_clinic_name  ,
                     event.date = as.character(Sys.time()),
                     action = paste0( " savePackagedDrugs -> insere um registo numa tabela Package"),
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
    
    
    temp_logs <- dbGetQuery( con.farmac , paste0("SELECT  unidade_sanitaria, data_evento, paciente, data_levantamento FROM logdispense where unidade_sanitaria='", clinic.name, "' ;" )  )
    
    return(temp_logs)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    # guardar o log 
    if(is.farmac){
      saveLogError(us.name =farmac_name ,
                   event.date = as.character(Sys.time()),
                   action = ' getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US',
                   error = as.character(cond$message) )  
      
    } else {
      
      # guardar o log 
      saveLogError(us.name = clinic.name,
                   event.date = as.character(Sys.time()),
                   action = ' getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US',
                   error = as.character(cond$message) )  
      
    }
    

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
#' con_farmac <- getLogErrorFromServer()
#' farmac_log_dispense <- getLogErrorFromServer(con_farmac,clinic_name)
#' 

getLogErrorFromServer <- function(con.farmac, clinic.name) {
  
  
  log_errors <- tryCatch({
    
    
    temp_logs <- dbGetQuery( con.farmac , paste0("SELECT  us, data_evento, accao, erro FROM  public.logerro where us = '", clinic.name, "' ;" )  )
    
    return(temp_logs)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    if(is.farmac){
      saveLogError(us.name =farmac_name ,
                   event.date = as.character(Sys.time()),
                   action = ' getLogErrorFromServer -> Busca os logs de erro de uma det. US ',
                   error = as.character(cond$message) )  
      
    } else {
      
      # guardar o log 
      saveLogError(us.name = clinic.name,
                   event.date = as.character(Sys.time()),
                   action = ' getLogErrorFromServer -> Busca os logs de erro de uma det. US ',
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
      if(is.farmac){
        saveLogError(us.name =farmac_name ,
                     event.date = as.character(Sys.time()),
                     action = ' getLogErrorFromServer -> Busca os logs de erro de uma det. US ',
                     error = as.character(cond$message) )  
        
      } else {
        
        # guardar o log 
        saveLogError(us.name = clinic.name,
                     event.date = as.character(Sys.time()),
                     action = ' getLogErrorFromServer -> Busca os logs de erro de uma det. US ',
                     error = as.character(cond$message) )  
      }
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
  
  log_errors
  
  
}





#' getMainClinicName -> Busca o nome da US na tabela clinic
#'
#' 
#' @param con.local  obejcto de conexao com BD iDART
#' @return nome da us
#' @examples 
#' main_clinic_name<- getMainClinicName(con_local)
#' 

getMainClinicName <- function(con.local) {
  
  
  clinic_name   <- dbGetQuery( con.local ,"select clinicname from clinic where mainclinic = TRUE ; " )
    
    return(clinic_name$clinicname[1])
 
  
}

#' getGenericProvider -> Busca o id do provedor generico
#'
#' @param con.local  obejcto de conexao com BD iDART
#' @return id da provider generico
#' @examples 
#' generic_provider <- getGenericProvider(con_local)
#' 

getGenericProviderID <- function(con.local) {
  
  
  provider   <- dbGetQuery( con.local ,"select id as provider from doctor where active = TRUE ; " )
  
  return(as.numeric(provider$provider[1]))
  
  
}



#' getRegimeID -> Busca o id do regime 
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @param regimeesquema nome do esquema
#' @return id do regime
#' @examples 
#' regimeid <- getRegimeID(con_local, 'TDF+3TC+EFV')
#' 
# 
# getRegimeID <- function(con.local, regimeesquema) {
#   
#   
#   regime   <- dbGetQuery( con.local ,
#                           paste0("select regimeid from regimeterapeutico  where regimeesquema = '",regimeesquema,"' and active = TRUE ; " ))
#   
#   if(nrow(regime)>0){
#     return(as.numeric(regime$regimeid[1]))
#   } else {
#     return(0)
#   }
#   
#   
# }

#' getLinhas -> Busca todas linhas T
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @examples 
#' linha <- getLinhaID(con_local)
#' 

getLinhas <- function(con.local) {
  
  
  linha   <- dbGetQuery( con.local ,paste0("select * from linhat ; " ) )

  return(linha)

}

 
#' getPatientInfo -> Busca o id , nid , nomes e uuid de todos pacientes do iDART
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @examples 
#' patients <- getPatientInfo(con_local )
#' 

getPatientInfo <- function(con.local) {
  
  patients   <- dbGetQuery(
    con.local ,
    paste0(
      "select pat.id, patientid, firstnames, lastname, uuid , uuidopenmrs, startreason, pi.value  from patient  pat 
      inner join patientidentifier pi on pi.patient_id = pat.id 
      left join
        (
         select patient, max(startdate), startreason
           from episode
            group by patient, startreason
        )  ep on ep.patient = pat.id ; "
    )
  )
  
  # Remover transitos
  patients <-  patients[which(!patients$startreason %in%  c('Paciente em Transito', ' Inicio na maternidade')), ]
  dfTemp <-  patients[which(grepl(   pattern = "TR", ignore.case = TRUE,  x = patients$patientid  ) == TRUE), ]
  dfTemp_2 <- patients[which(grepl(  pattern = "VIS",ignore.case = TRUE,  x = patients$patientid ) == TRUE), ]
  dfTemp_3 <- patients[which(grepl( pattern = "VIA",  ignore.case = TRUE,  x = patients$patientid  ) == TRUE), ]

  
  patients <-  patients[which(!patients$patientid %in% dfTemp$patientid), ]
  patients <-    patients[which(!patients$patientid %in% dfTemp_2$patientid), ]
  patients <-  patients[which(!patients$patientid %in% dfTemp_3$patientid), ]

  
  rm(dfTemp_2, dfTemp, dfTemp_3)
  
  
  return(patients)
  
}


#' getLastPackageDrugInfoTmpID -> Busca o utlimo  id da tabela PackageDrugInfoTmp  na BD
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @return id do ultimo registo em  PackageDrugInfoTmp 
#' @examples 
#' id <- getLastPackageDrugInfoTmpID(con_local)
#' 

getLastPackageDrugInfoTmpID <- function(con.local) {
  
  
  id    <- dbGetQuery( con.local ,
                          paste0("SELECT id  FROM packagedruginfotmp order by id desc limit 1 ; " ))
  
  if(nrow(id)>0){
    return(as.numeric(id$id[1]))
  } else {
    return(0)
  }
  
  
}

#' getLastKnownRegimeID -> Busca o id do regime da ultima precricao na BD
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @param patient.id nid
#' @return id do regime
#' @examples 
#' regimeid <- getLastKnownRegimeID(con_local, '12/566')
#' 

getLastKnownRegimeID <- function(con.local,patient.id) {
  
  
  regime   <- dbGetQuery( con.local ,
                          paste0("SELECT regimeid  FROM prescription where patient = ",
                          as.numeric(patient.id),
                          " order by date desc limit 1; " ))
  
  if(nrow(regime)>0){
    return(regime)
    #return(as.numeric(regime$regimeid[1]))
  } else {
    return(0)
  }
  
  
}



#' getLastPrescriptionID -> Busca o id da ultima precricao na BD
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @return id da ultima precricao
#' @examples 
#' precricaoid <- getLastPrescriptionID(con_local)
#' 

getLastPrescriptionID <- function(con.local) {
  
  
  prescriptionid   <- dbGetQuery( con.local ,
                          paste0("SELECT id  FROM prescription order by id desc limit 1; " ))
  
  if(nrow(prescriptionid)>0){
    return(as.numeric(prescriptionid$id[1]))
  } else {
    return(0)
  }
  
  
}


#' getLastPrescribedDrugID -> Busca o id da ultima getLastPrescribedDrug na BD
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @return id do PrescribedDrug
#' @examples 
#' precricaoid <- getLastPrescribedDrugID(con_local)
#' 

getLastPrescribedDrugID <- function(con.local) {
  
  
     prescribeddrug   <- dbGetQuery( con.local ,
                                  paste0("SELECT id  FROM prescribeddrugs order by id desc limit 1; " ))
  
  if(nrow(prescribeddrug)>0){
    return(as.numeric(prescribeddrug$id[1]))
  } else {
    return(0)
  }
  
  
}






#' getRegimeID -> Busca o id do regime com base no nome do regime
#'
#' @param df.regimes df com regimes padronizados
#' @param regime.name name
#' @return id do regime
#' @examples 
#' regimeid <- getRegimeID(con_local, '12/566')
#' 

getRegimeID <- function(df.regimes ,regime.name) {
  
  regime  <- df.regimes[which(df.regimes$regimeesquema==regime.name),]
  if(nrow(regime)>0){
    if(nrow(regime)==1){
      return(regime)
    } else{
      
      tmp_regime= subset( df.regimes, regimeesquema==regime.name & active == TRUE ,)
      return(tmp_regime[1,])
    }
  } else{  # vamos procurar pela abreviatura ex : ABC+3TC+LPV/r(2DFC+LPV/r40/10) ->  ABC+3TC+LPV/r
    
    size <- nchar(regime.name)
    if(size>11){
      regime.name <- substr(regime.name, 1, 13 )
    }
    regime = subset(df.regimes, grepl(pattern = regime.name,x = df.regimes$regimeesquema,ignore.case = TRUE),)
    
    if(nrow(regime)==1){
      return(regime)
    } else if(nrow(regime)>1){
      dosage <- sub(".*(\\d+{2}).*$", "\\1", regime.name)
      
      regime = subset(df.regimes, grepl(pattern = regime.name,x = df.regimes$regimeesquema,ignore.case = TRUE) & grepl(pattern = dosage,x = df.regimes$regimeesquema,ignore.case = TRUE),)
      if(nrow(regime)>1) {
        return(regime[1,])
      } else {
        return(0)
      }

    } else { # pega o ulimo regime conhecido (ideia do colaco kkkk)
      return(0)
    }
  }
  
}

#' getLinhaID -> Busca o id da Linha 
#'
#' @param df.linhas  df com todos linhas T
#' @param linha  nome  da linha
#' @return id 
#' @examples 
#' id  <- getLinhaID(df.linhas,'1a Linha' ))
#' 

getLinhaID <- function(df.linhas,linha) {
  
  id <- df.linhas$linhaid[which(df.linhas$linhanome==linha)]
  id
}
  
#' getDrug -> Busca o drug pelo nome
#'
#' @param df.drugs  df com todos drugs T
#' @param linha  nome  da linha
#' @return id 
#' @examples 
#' id  <- getDrugId(df.drugs,'[LPV/RTV]' ))
#' 

getDrug<- function(df.drugs,drug.name) {
  
  drug <- df.drugs[which(df.drugs$name==drug.name),]
  if(nrow(drug)>0){
    if(nrow(drug)==1){
      return(drug)
    } else{
      
      tmp_drug = subset( df.drugs, name==drug.name & active == TRUE & pediatric =='F',)
      return(tmp_drug)
    }
  } else{  # vamos procurar pela abreviatura que esta dentro de parentesis rectos [TDF/3TC/DTG]
    
    first_index <- stri_locate_first(drug.name, regex = "\\[")[[1]]
    second_index <- stri_locate_first(drug.name, regex = "\\]")[[1]]
    dosage <- sub(".*(\\d+{2}).*$", "\\1", drug.name)
    
     abreviatura_drug <- substr(drug.name, first_index+1, second_index-1 )
     
     drug = subset(df.drugs, grepl(pattern = abreviatura_drug,x = df.drugs$name,ignore.case = TRUE),)
     if(nrow(drug)==1){
       return(drug)
     } else if(nrow(drug)>1){
       drug = subset(df.drugs, grepl(pattern = abreviatura_drug,x = df.drugs$name,ignore.case = TRUE) & df.drugs$pediatric=='F' ,)
       return(drug[1,])
     } else { # pega qualquer (ideia do colaco kkkk)
       return(df.drugs[1,])
     }
  }
}

#' getAllDrugs -> Busca todos medicamentos do iDART 
#'
#' @param con.postgres  objecto de conexao com  a bd
#' @return public.drugs 
#' @examples 
#' id  <- getAllDrugs(con_local)
#' 
# 
# getAllDrugs <- function(con.postgres) {
#   
#   
#   drugs   <- dbGetQuery( con.local , paste0("SELECT * from drug ; " ))
#   return(drugs)
#   
# }


#' getLastPrescriptionID -> Buscao id da ultima prescricao criada
#'
#' @param con.postgres  objecto de conexao com  a bd
#' @return id
#' @examples 
#' id  <- getLastPrescriptionID(con_local)
#' 

getLastPrescriptionID <- function(con.postgres) {
  
  
  id <- dbGetQuery( con.postgres , paste0("select id from prescription order by id desc limit 1; " ))
  return(id$id)
  
}

#' getLastPackageID -> Busca id do ultimo package criado
#'
#' @param con.postgres  objecto de conexao com  a bd
#' @return id dao ultimo registo em package
#' @examples 
#' id  <- getLastPackageID(con_local)
#' 

getLastPackageID <- function(con.postgres) {
  
  
  id <- dbGetQuery( con.postgres , paste0("select id from package order by id desc limit 1; " ))
  return(id$id)
  
}

#' getLastPackagedDrugsID -> Busca id do ultimo packageddrugs criado
#'
#' @param con.postgres  objecto de conexao com  a bd
#' @return id dao ultimo registo em packageddrugs
#' @examples 
#' id  <- getLastPackagedDrugsID(con_local)
#' 

getLastPackagedDrugsID <- function(con.postgres) {
  
  
  id <- dbGetQuery( con.postgres , paste0("select id from packageddrugs order by id desc limit 1; " ))
  return(id$id)
  
}




#' getPatientId -> Busca o id do paciente 
#'
#' @param df.temp.patients  df com todos pacientes do idart excluindo os transitos
#' @param patient c(nid,firstnames,lastname)
#' @return id 
#' @examples 
#' id  <- getPatientId(con_local, c('12/3444','Agnaldo Samuel', 'Macuacua'))
#' 

getPatientId <- function(df.temp.patients,patient) {
  
  nid <- patient[1]
  
 id <- df.temp.patients$id[which(df.temp.patients$patientid==nid)][1]
 if(is.na(id)){
   id <- df.temp.patients$id[which(df.temp.patients$value==nid)][1]
   if(is.na(id)){
     pat <- subset(df.temp.patients,firstnames==patient[2]  & lastname ==patient[3])
     if(nrow(pat)==1){
       id <- pat$id[1]
       return(id)
       
     } else if(nrow(pat)>1){# paciente duplicado
       
       if(pat$uuid[1]==pat$uuid[2]){ # sao pacientes iguais, considere o primeiro
         
         id <- pat$id[1]
         
         if(pat$uuid[1]==pat$uuid[2]){
           
           # historico clinico de pacientes : Novo Paciente   ->  Referido para outra Farmacia  ->  Voltou da Referencia 
          
         } else {  # sao pacientes duplicados
           
           if(is.farmac){
             
             saveLogError( us.name = farmac_name,    event.date =as.character(Sys.Date()),
                           action = 'getPatientId -> Busca o id do paciente',
                           error =  paste0('ERR_DUP Paciente FARMAC duplicado - ',pat$patientid[1])      )
           } else {
             saveLogError( us.name = main_clinic_name,    event.date = as.character(Sys.Date()),
                           action = 'getPatientId -> Busca o id do paciente',
                           error =  paste0('ERR_DUP Paciente FARMAC duplicado - ',pat$patientid[1])      )
           }
           
           
           
         }

         
         return(id)
         
       } else {
         
         
         if(is.farmac){
           
           saveLogError( us.name = farmac_name,    event.date =as.character(Sys.Date()),
                         action = 'getPatientId -> Busca o id do paciente',
                         error =  paste0('ERR_DUP Paciente FARMAC duplicado - ',pat$patientid[1])      )
         } else {
           saveLogError( us.name = main_clinic_name,    event.date = as.character(Sys.Date()),
                         action = 'getPatientId -> Busca o id do paciente',
                         error =  paste0('ERR_DUP Paciente FARMAC duplicado - ',pat$patientid[1])      )
         }
         
         return(0)  # error
         
       }

         
     } else {
       
       return(0)  # no patient found
     }
     
   } else{
       return(id)
     }

 }else{
   return(id)
 }

}




#' composePrescription -> compoe um dataframe de prescricao para actualizar
#'
#' @param df.dispense  df dispensa do paciente
#' @param patient.id id do paciente
#' @param provider.id  id do doctor
#' @param linha.id id linha
#' @param prescription.id id da prescricao -> gerado aleatoriamente
#' @return NA 
#' @examples 
#' prescription  <- composePrescription(df.dispense,linha.id, regime.id,provider.id, patient.id)
#' 

composePrescription <- function(df.dispense,linha.id, regime.id,provider.id, patient.id, nid,prescription.id) {
 
  load('config/prescription.Rdata')


  precription <- add_row( precription,
                         clinicalstage      =0,
                         current            = 'T',
                         weight             = 0,
                         date               = df.dispense$date[1],
                         doctor             =provider.id,
                         duration           =  df.dispense$duration[1],
                         modified           =   df.dispense$modified[1],
                         patient            = patient.id,
                         prescriptionid     =    paste0(nid, '-', gsub(pattern = ' ',replacement = '_', x =df.dispense$date[1] ),"_Farmac"  )  ,
                         reasonforupdate    = df.dispense$reasonforupdate[1],
                         notes              = paste0( 'FARMAC-',gsub (pattern = 'NA',replacement = '',x = df.dispense$notes[1])), 
                         enddate            = df.dispense$enddate[1],    
                         drugtypes          = df.dispense$drugtypes[1],    
                         regimeid           = regime.id,
                         datainicionoutroservico = df.dispense$datainicionoutroservico[1],  
                         motivomudanca      = df.dispense$motivomudanca[1], 
                         linhaid            = linha.id,
                         dispensatrimestral = df.dispense$dispensatrimestral[1],
                         ppe                = df.dispense$ppe[1],   
                         ptv                = df.dispense$ptv[1],  
                         tb                 = df.dispense$tb[1],  
                         tpi                = df.dispense$tpi[1],   
                         tpc                = df.dispense$tpc[1],
                         gaac              = df.dispense$gaac[1], 
                         af                 = df.dispense$af[1],    
                         ca                 = df.dispense$ca[1],         
                         ccr                = df.dispense$ccr[1], 
                         saaj               = df.dispense$saaj[1],   
                         fr                 = df.dispense$fr[1] ,
                         id                 = prescription.id)
 
   
     return(precription)

  
}



#' composePrescribedDrugs -> compoe um dataframe de  um prescribeddrugs drgus para inserir 
#'
#' @param df.dispense  df dispensa do paciente
#' @param prescription.id  id da prescicao
#' @param prescribed.drug.id id por inserir na tabela
#' @return NA 
#' @examples 
#' df_prescruibed_drugs  <- composePrescribedDrugs(df.dispense, prescription.id, prescribed.drug.id)
#' 

composePrescribedDrugs <- function(df.dispense, prescription.id) {
  
  load('config/prescribeddrugs.RData')
 
  temp_prescribeddrugs <- prescribeddrugs
    
  for(i in 1:nrow(df.dispense)){

    prescribed_drug_id <- getLastPrescribedDrugID(con_local)
    prescribed_drug_id <- prescribed_drug_id + sample(7:23, 1)*3 + 2*i ##  gerao aleatoria de ID apartir do ultimo
    
    drug_name <- df.dispense$drugname[i]
    drug <- getDrug(df.drugs =drugs,drug.name =drug_name)
    amt =0
    if(drug$packsize[1]>30){
      amt =2
    } else {
      amt =1
    }
    prescribeddrugsindex = i -1
    
    temp_prescribeddrugs <- add_row(temp_prescribeddrugs,
      amtpertime  =amt,
      drug = drug$id[1]  ,
      prescription = prescription.id,
      timesperday =df.dispense$timesperday[i],
      modified =  df.dispense$modified[i] ,
      prescribeddrugsindex = prescribeddrugsindex,
      id = prescribed_drug_id
      )
    
   # temp_prescribeddrugs <- rbind.fill(prescribeddrugs,temp_prescribeddrugs )
  }
  
  return(temp_prescribeddrugs)
  
}



#' composePackageOpenmrs -> compoe um dataframe com informacao para enviar para openmrs 
#'
#' @param df.packagedruginfotmp   df dispensa do paciente
#' @return df com info da para packageOpenmrs 
#' @examples 
#' df_prescruibed_drugs  <- composePackageOpenmrs(df.packagedruginfotmp)
#' 

composePackageOpenmrs <- function(df.packagedruginfotmp) {
  

  load(file = 'config/package.RData' )
  temp_package <- package
  temp_package <- add_column(temp_package,amount='')
  temp_package <- add_column(temp_package,customizedDosage='')
  temp_package <- add_column(temp_package,dosage='')
  temp_package$amount <- as.double(  temp_package$amount )
  temp_package$dosage <- as.integer(  temp_package$dosage )
  
  #temp_package$amtpertime  <- ""
  # 
  drug_name <- df.packagedruginfotmp$drugname[i]
  drug <- getDrug(df.drugs =drugs,drug.name =drug_name)
  amt = 0 ## default

  if(drug$packsize[1]>30){
    amt =2
  } else {
    amt =1
  }
  
  for (v in 1:nrow(df.packagedruginfotmp)) {

      id_package <- 3*sample(11:19, 1) + 17*v  # random id generation
      temp_package <- add_row(
      temp_package,
      id = id_package,
      dateleft = df.packagedruginfotmp$dispensedate[v],
      datereceived = df.packagedruginfotmp$dispensedate[v],
      packdate =  df.packagedruginfotmp$dispensedate[v],
      pickupdate = df.packagedruginfotmp$dispensedate[v],
      clinic = 2,   # warning, this is  hardcoded
      weekssupply = df.packagedruginfotmp$weekssupply[v],
      drugtypes = 'ARV',
      amount = df.packagedruginfotmp$dispensedqty[v],
      dosage = df.packagedruginfotmp$timesperday[v],
      customizedDosage = paste0('Tomar ', df.packagedruginfotmp$timesperday[v] , ' Comp', amt, ' vezes por dia' )
      
   
    )
    
  }
  
  return(temp_package)
  
}


#' composePackage -> compoe um dataframe de um package  para inserir 
#'
#' @param df.dispense  df dispensa do paciente
#' @param prescription.id  id da prescicao
#' @param prescribed.drug.id id por inserir na tabela
#' @return NA 
#' @examples 
#' df_prescruibed_drugs  <- composePackage(df.packagedruginfotmp, prescription.to.save)
#' 

composePackage <- function(df.packagedruginfotmp, prescription.to.save) {
  
  
  load(file = 'config/package.RData')
  temp_package <- package
  
  for (v in 1:nrow(df.packagedruginfotmp)) {
    id_package <- getLastPackageID(con_local)
    id_package <-
      id_package + 3*sample(11:19, 1) + 17*v  # random id generation
    
    temp_package <- add_row(
      temp_package,
      id = id_package,
      dateleft = df.packagedruginfotmp$dispensedate[v],
      datereceived = df.packagedruginfotmp$dispensedate[v],
      modified = 'T',
      packageid = prescription.to.save$prescriptionid[v],
      packdate =  prescription.to.save$date[v],
      pickupdate = df.packagedruginfotmp$dispensedate[v],
      prescription = prescription.to.save$id[1],
      clinic = 2,
      # warning, this is  hardcoded
      weekssupply = df.packagedruginfotmp$weekssupply[v],
      drugtypes = 'ARV',
      stockreturned = FALSE,
      packagereturned = FALSE,
      reasonforpackagereturn = '',
      datereturned = NA
    )
    
    
}

return(temp_package)
  
}

#' composePackagedDrugs -> compoe um dataframe de um composePackagedDrug  para inserir 
#'
#' @param df.packagedruginfotmp  df dispensa do paciente
#' @param package.to.save  package associado 
#' @param packageddrugsindex index do packagedrug
#' @return df composePackagedDrugs 
#' @examples 
#' df_packageddrugs  <- composePackagedDrugs(df.packagedruginfotmp, package.to.save, packageddrugsindex)
#' 

composePackagedDrugs <- function(df.packagedruginfotmp, package.to.save ) {
  
  # carrega df vazio packageddrugs
  load(file = 'config/packageddrugs.RData')
  temp_packageddrugs <- packageddrugs
  
  for (v in 1:nrow(df.packagedruginfotmp)) {
   
    id_pd <- getLastPackagedDrugsID(con_local)
    id_pd <-  id_pd + 6*sample(11:16, 1) + 26*v  # random id generation
    packageddrugsindex <- v - 1
    
    temp_packageddrugs <- add_row(temp_packageddrugs,
                            id = id_pd,
                            amount = df.packagedruginfotmp$dispensedqty[v],
                            parentpackage= package.to.save$id[v] ,
                            stock=df.packagedruginfotmp$stockid[v],
                            modified='T',
                            packageddrugsindex = packageddrugsindex)

  }
 
return(temp_packageddrugs)
  
}


#' composePackageDrugInfoTmp -> compoe um dataframe de um package  para inserir 
#'
#' @param df.dispense  df dispensa do paciente
#' @param prescription.id  id da prescicao
#' @param prescribed.drug.id id por inserir na tabela
#' @return NA 
#' @examples 
#' df_prescruibed_drugs  <- composePackageDrugInfoTmp(df.patient.dispenses, prescription.id, prescribed.drug.id)
#' 

composePackageDrugInfoTmp <- function(df.patient.dispenses, user.id) {
  
  load(file = 'config/packagedruginfotmp.RData') 
  temp_packagedruginfotmp <- packagedruginfotmp
  
  for(i in 1:nrow(df.patient.dispenses)){
  
    # para cada drug associar um stock
    drug_name <- df.patient.dispenses$drugname[i]
    drug <- getDrug(df.drugs = drugs,drug_name )
    stock <- getStockForDrug(con_local,drug$id[1])
    id <- getLastPackageDrugInfoTmpID(con_local)
    id <- id + sample(7:17, 1)*7 +i
    
    temp_packagedruginfotmp <- add_row(
      temp_packagedruginfotmp,
      id = id,
      amountpertime = '0',
      clinic = main_clinic_name,
      dispensedqty = 0,
      batchnumber = "",
      formlanguage1 = "",
      formlanguage2 = "",
      formlanguage3 = "",
      drugname = drug_name,
      expirydate = df.patient.dispenses$expirydate[i],
      patientid = df.patient.dispenses$patientid[i],
      patientfirstname = df.patient.dispenses$patientfirstname[i],
      patientlastname= df.patient.dispenses$patientlastname[i],
      specialinstructions1 = "",
      specialinstructions2 = "",
      stockid = stock$id[1],
      timesperday= df.patient.dispenses$timesperday[i],
      numberoflabels =0,
      cluser = user.id,
      dispensedate = df.patient.dispenses$dispensedate[i],
      weekssupply= df.patient.dispenses$weekssupply[i],
      qtyinhand = df.patient.dispenses$qtyinhand[i],
      summaryqtyinhand = df.patient.dispenses$summaryqtyinhand[i],
      qtyinlastbatch = df.patient.dispenses$qtyinlastbatch[i],
      prescriptionduration = df.patient.dispenses$duration[i],
      dateexpectedstring = df.patient.dispenses$dateexpectedstring[i],
      pickupdate = df.patient.dispenses$pickupdate[i],
      notes = ""
    )
    

  }
  
  return(temp_packagedruginfotmp)

}

#' composePackageDrugInfoTmpOpenMRS -> compoe um dataframe de um package  para enviar para openmrs
#'
#' @param df.dispense  df dispensa do paciente
#' @param prescription.id  id da prescicao
#' @param prescribed.drug.id id por inserir na tabela
#' @return NA 
#' @examples 
#' df_prescruibed_drugs  <- composePackageDrugInfoTmpOpenMRS(df.patient.dispenses, prescription.id, prescribed.drug.id)
#' 

composePackageDrugInfoTmpOpenMRS <- function(df.patient.dispenses, user.id, regime.uuid) {

  load(file = 'config/packagedruginfotmp.RData')
  
  temp_packagedruginfotmp <- packagedruginfotmp

  temp_packagedruginfotmp <- temp_packagedruginfotmp[ , -which( names(temp_packagedruginfotmp)
                                                                %in% c("batchnumber",
                                                                       'formlanguage1',
                                                                       'formlanguage2',
                                                                       'formlanguage3',
                                                                       'specialinstructions1',
                                                                       'specialinstructions2',
                                                                       'stockid',
                                                                       'numberoflabels',
                                                                       'notes',
                                                                       'stockid',
                                                                       'numberoflabels',
                                                                       'id',
                                                                       'expirydate',
                                                                       'drugname',
                                                                       'clinic',
                                                                       'packageindex',
                                                                       'invalid',
                                                                       'senttoekapa',
                                                                       'sidetreatment',
                                                                       'dispensedforlaterpickup',
                                                                       'qtyinlastbatch',
                                                                       'firstbatchinprintjob'
                                                                       ))]
  
  


  temp_packagedruginfotmp <- add_column(temp_packagedruginfotmp,customizedDosage='')
  temp_packagedruginfotmp <- add_column(temp_packagedruginfotmp,regimeuuid='')
  temp_packagedruginfotmp$dateexpectedstring <-as.Date(temp_packagedruginfotmp$dateexpectedstring)

  for(i in 1:nrow(df.patient.dispenses)){

    # para cada drug associar um stock
    drug_name <- df.patient.dispenses$drugname[i]
    drug <- getDrug(df.drugs = drugs,drug_name )
    if(nchar(df.patient.dispenses$summaryqtyinhand)[i]>4){
      qty = as.integer(substr(df.patient.dispenses$summaryqtyinhand,2,4))
    } else{
      
      qty = as.integer(substr(df.patient.dispenses$summaryqtyinhand,2,3))
    }
    nr_toma <- 1
    if(as.integer(qty)>30){
      nr_toma <- 2
    }

    temp_packagedruginfotmp <- add_row(
      temp_packagedruginfotmp,
      patientid = df.patient.dispenses$patientid[i],
      patientfirstname = df.patient.dispenses$patientfirstname[i],
      patientlastname= df.patient.dispenses$patientlastname[i],
      timesperday= df.patient.dispenses$timesperday[i],
      cluser = user.id,
      dispensedqty = qty,
      dispensedate = df.patient.dispenses$dispensedate[i],
      weekssupply= df.patient.dispenses$weekssupply[i],
      qtyinhand = df.patient.dispenses$qtyinhand[i],
      summaryqtyinhand = df.patient.dispenses$summaryqtyinhand[i],
      prescriptionduration = df.patient.dispenses$duration[i],
      dateexpectedstring = df.patient.dispenses$dateexpectedstring[i],
      pickupdate = df.patient.dispenses$pickupdate[i],
      regimeuuid = regime.uuid,
      customizedDosage= paste0('Tomar ', df.patient.dispenses$timesperday[i],' Comp ',nr_toma,' vezes por dia')
    )


  }

  return(temp_packagedruginfotmp)

}


#' getStockForDrug -> retorna o stock existente de um drug
#' se nao encontrar o stock associado ao drug retorna qualquer um
#' @param con.postgres  con com BD
#' @param drug.id  id da drug
#' @return stock 
#' @examples 
#' stock  <- getStockForDrug(con.postgres,drug.id)
#' 

  getStockForDrug <- function(con.postgres, drug.id) {
  
 stock_list <- dbGetQuery(con.postgres, paste0("select * from stock where drug = ",
                                           drug.id,
                                          " order by expirydate ASC, datereceived ASC, id ASC"))
 
 if(nrow(stock_list)==0){
   
   stock <- dbGetQuery(con.postgres, paste0("select * from  stock where hasunitsremaining='T'; "))
   stock <- stock[1,]
 } else {
   
   stock = stock_list[1,]
 }

 
 return(stock)
  
  }
  
  
  
  
  #' getAdminUser -> retorna a informacao do user admin no iDART
  #' @param con.postgres  con com BD
  #' @return admin 
  #' @examples 
  #' user_admin  <- getAdminUser(con.postgres)
  #' 
  
  getAdminUser <- function(con.postgres) {
    
    admin <- dbGetQuery(con.postgres, paste0("select * from users where cl_username = 'admin' ;"))

    
    return(admin)
    
  }
  
  
  #' ReadJdbcProperties ->  carrega os paramentros de conexao no ficheiro jdbc.properties
  #' @param file  patg to file
  #' @return  vec [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
  #' @examples 
  #' user_admin  <- ReadJdbcProperties(file)
  #' 
  
  readJdbcProperties <- function(file='config/jdbc.properties') {


    vec <- as.data.frame(read.properties(file = file ))
    vec
  }
  
  #' checkPatientUuidExistsOpenMRS ->  verifica se existe um paciente no openmrs com um det. uuidopenmrs
  #' @param jdbc.properties  [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
  #' @return  TRUE/FALSE 
  #' @examples 
  #' status  <- checkPatientUuidExistsOpenMRS(jdbc.properties, c("0102010001/2006/04892","julia" ,"jaoquina", "cb90174b-81e9-43e4-9b4d-dc09d2966efa"))
  #' 
  
  checkPatientUuidExistsOpenMRS <- function(jdbc.properties, patient) {
    # url da API
    base.url.rest <- as.character(jdbc.properties$urlBaseReportingRest)
    base.url <-  as.character(jdbc.properties$urlBase)
    status <- TRUE
    url.check.patient <- paste0(base.url,'person/',patient[4])
    
    r <- content(GET(url.check.patient, authenticate('farmac', 'iD@rt2020!')), as = "parsed")
    
    if("error" %in% names(r)){
      if(r$error$message =="Object with given uuid doesn't exist" ){
        saveLogError(us.name = main_clinic_name,
                     event.date = as.character(Sys.time()),
                     action = paste0("sendFilaOpenMRS - ",patient[1],' - ', patient[4]),
                     error ="Object with given uuid doesn't exist" )
      }
      status<-FALSE
      return(FALSE)
      
    } else{
      
      return(status)
      
    }
    
    return(status)
  }
  
  
  #' sendFilaOpenMRS ->  envia fila para openmrs usanda a restAPI
  #' @param file  patg to file
  #' @return  vec [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
  #' @examples 
  #' user_admin  <- ReadJdbcProperties(file)
  #' 
  
  sendFilaOpenMRS <- function(jdbc.properties,df.patient, df.fila.info) {
    
    
    encounterType = 'e279133c-1d5f-11e0-b929-000c29ad1d07' # FILA
    providerUuid ="7013d271-1bc2-4a50-bed6-8932044bc18f" # generic provider
    regimeUuid = "e1d83e4e-1d5f-11e0-b929-000c29ad1d07"
    dispensedAmountUuid ="e1de2ca0-1d5f-11e0-b929-000c29ad1d07"
    filaUuid = "49857ace-1a92-4980-8313-1067714df151"
    dosageUuid= "e1de28ae-1d5f-11e0-b929-000c29ad1d07"
    returnVisitUuid="e1e2efd8-1d5f-11e0-b929-000c29ad1d07"
    
    
    encounterDatetime =as.Date(df.fila.info$dispensedate[1])
    nidUuid = df.patient[4]
    strRegimenAnswerUuid = df.fila.info$regimeuuid[1]
    strFacilityUuid = as.character(jdbc_properties$location)
    packSize =  df.fila.info$dispensedqty[1]
    customizedDosage = df.fila.info$customizedDosage[1]
    strNextPickUp = df.fila.info$dateexpectedstring[1]
    
    # url da API
    base.url <-  as.character(jdbc.properties$urlBase)
    base.url <- str_c(base.url,'encounter')
    status <- TRUE
  
    
    string <- str_c("{\"encounterDatetime\": \"" , encounterDatetime , "\", \"patient\": \""
                    , nidUuid , "\", \"encounterType\": \"" , encounterType , "\", " , "\"location\":\""
                    , strFacilityUuid , "\", \"form\":\"" , filaUuid , "\", \"encounterProviders\":[{\"provider\":\""
                    , providerUuid , "\", \"encounterRole\":\"a0b03050-c99b-11e0-9572-0800200c9a66\"}], "
                    , "\"obs\":[{\"person\":\"" , nidUuid , "\",\"obsDatetime\":\"" , encounterDatetime
                    , "\",\"concept\":" , "\"" , regimeUuid , "\",\"value\":\"" , strRegimenAnswerUuid
                    , "\", \"comment\":\"IDART\"},{\"person\":" , "\"" , nidUuid , "\",\"obsDatetime\":\""
                    , encounterDatetime , "\",\"concept\":\"" , dispensedAmountUuid , "\"," , "\"value\":\"" , packSize
                    , "\",\"comment\":\"IDART\"},{\"person\":\"" , nidUuid , "\",\"obsDatetime\":\"" , encounterDatetime
                    , "\",\"concept\":" , "\"" , dosageUuid , "\",\"value\":\"" , customizedDosage
                    , "\",\"comment\":\"IDART\"},{\"person\":\"" , nidUuid , "\"," , "\"obsDatetime\":\""
                    , encounterDatetime , "\",\"concept\":\"" , returnVisitUuid , "\",\"value\":\"" , strNextPickUp
                    , "\",\"comment\":\"IDART\"}]}")
    
    

    r <- POST(url = base.url, body = string, config=authenticate('farmac', 'iD@rt2020!'),  add_headers("Content-Type"="application/json") )
    
    if(as.integer(r$status_code)==201 | as.integer(r$status_code) == 204){
        message('Fila inserido no openmrs')
      return(TRUE)
    } else {
      
      message( paste0("Erro ao enviar fila do paciente: nid",df.patient[1], " - uuid:",df.patient[4] ))
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = 'sendFilaOpenMRS',
                   error = paste0("Erro ao enviar fila do paciente: nid",df.patient[1], " - uuid:",df.patient[4] ))
      save(logErro,file = 'logs/logErro.RData')
      print(as.character(r$error$message))
      return(FALSE)
    }

  }
  