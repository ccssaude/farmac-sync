#' getFarmacSyncTempDispense -> Busca Dispensas de uma det. US na tabela sync_temp_dispense
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.farmac  obejcto de conexao com BD iDART
#' @param farmac.name nome da farmac que enviou as dispenas para o servidor 
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas de determinada US
#' @examples 
#' farmac.name <- 'farmac jardim'
#' con_farmac <- getFarmacServerCon()
#' farmac_temp_sync_dispense<- getFarmacSyncTempDispense(con_farmac,farmac.name)
#' 

getFarmacSyncTempDispense <- function(con.farmac, farmac.name) {
  
  
  sync_temp_dispense <- tryCatch({

    
    sync_temp_dispense  <- dbGetQuery( con.farmac , paste0("select * from public.sync_temp_dispense
                                                         where clinic_name_farmac ='", farmac.name, "' ;" )  )
    return(sync_temp_dispense)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.Date()),
                 action = ' getFarmacSyncTempDispense -> Busca Dispensas de uma det. US na tabela sync_temp_dispense no servidor FARMAC PosgreSQL ',
                 error =as.character(cond$message) )  
    
    #Choose a return value in case of error
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  sync_temp_dispense
  
  
}


#' getLocalSyncTempDispense -> Busca Dispensas da US local na tabela sync_temp_dispense
#' 
#' @param con.local  obejcto de conexao com BD iDART
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas  da US
#' @examples 
#' main.clinic.name <- 'CS Albazine'
#' con_farmac <- getLocalServerCon()
#' local_temp_sync_dispense<- getLocalSyncTempDispense(con_farmac,main.clinic.name)
#' 

getLocalSyncTempDispense <- function(con.local) {
  
  
  sync_temp_dispense <- tryCatch({
    
    
    sync_temp_dispense  <- dbGetQuery( con.local , paste0("select * from public.sync_temp_dispense ;" )  )
    return(sync_temp_dispense)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.Date()),
                 action = ' getLocalSyncTempDispense -> Busca Dispensas da US local na tabela sync_temp_dispense ',
                 error =as.character(cond$message) )  
    
    #Choose a return value in case of error
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  sync_temp_dispense
  
  
}





#' sendDispenseToServer -> Envia dispensas dos pacientes da farmac para o servidor Servidor Farmac 
#' 
#' @param con.farmac  obejcto de conexao com BD
#' @param df.dispenses o datafrane com as dispensas dos pacientes referidos (apenas as novas)  
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- sendDispenseToServer(con_farmac,pacientes_referidos)


sendDispenseToServer <- function(con.farmac ,df.dispenses ){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    dbWriteTable(con.farmac, "sync_temp_dispense", df.dispenses, row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status <- TRUE
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.Date()),
                 action = ' sendDispenseToServer ->  Envia dispensas dos pacientes da farmac para o servidor Servidor Farmac ',
                 error = as.character(cond$message) )  
    
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

