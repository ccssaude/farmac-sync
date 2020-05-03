

#' getFarmacSyncTempPatients -> Busca pacientes de uma det. US na tabela sync_temp_patient 
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.farmac  obejcto de conexao com BD iDART
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
#' @param con.local  obejcto de conexao com BD iDART
#' @param main.clinic.name nome da us 
#' @return tabela/dataframe/df com todos pacientes da tabela sync_temp_pacientes da US
#' @examples 
#' main.clinic.name <- 'CS Albazine'
#' con_local <- getLocalServerCon()
#' sync_patients_farmac <- getLocalSyncTempPatients(con_local,main.clinic.name)
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

refferPatients <- function(con.farmac, reffered.patients) {
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    dbWriteTable(con_farmac, "sync_temp_patients", reffered.patients, row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status <- TRUE
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
      message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.time()),
                 action = ' refferPatients -> Envia pacientes referidos para o Servidor Farmac',
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
