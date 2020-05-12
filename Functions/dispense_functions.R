library(dplyr)
library(plyr)

#' getFarmacSyncTempDispense -> Busca Dispensas de uma det. FARMAC na tabela sync_temp_dispense
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.farmac  obejcto de conexao com BD iDART
#' @param farmac.name nome da farmac que enviou as dispenas para o servidor 
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas de determinada US
#' @examples 
#' farmac.name <- 'farmac jardim'
#' con_farmac <- getFarmacServerCon()
#' farmac_temp_sync_dispense<- getFarmacSyncTempDispenseByName(con_farmac,farmac.name)
#' 

getFarmacSyncTempDispense <- function(con.farmac, farmac.name) {
  
  
  sync_temp_dispense <- tryCatch({

    
    sync_temp_dispense  <- dbGetQuery( con.farmac , paste0("select * from sync_temp_dispense
                                                         where clinic_name_farmac ='", farmac.name, "' ;" )  )
    return(sync_temp_dispense)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.time()),
                 action = 'getFarmacSyncTempDispense -> Busca Dispensas de uma det. US na tabela sync_temp_dispense no servidor FARMAC PosgreSQL ',
                 error = as.character(cond$message) )  
    
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
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = 'Warning getUsSyncTempDispense -> Busca Dispensas de uma det. US na tabela sync_temp_dispense no servidor FARMAC PosgreSQL ',
                   error = as.character(cond$message) )  
      
      return(FALSE)
      
    } else {
      
      if (exists('sync_temp_dispense')){
        
        return(sync_temp_dispense)
      }
      
      
    }
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  sync_temp_dispense
  
  
}

#' getUsSyncTempDispense -> Busca Dispensas pertencentes a uma det. US na tabela sync_temp_dispense
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.farmac  obejcto de conexao com BD iDART
#' @param clinic.name nome da US que referiu os pacientes 
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas de determinada US
#' @examples 
#' clinic_name <- 'CS BAGAMOIO'
#' con_farmac <- getFarmacServerCon()
#' farmac_temp_sync_dispense<- getUsSyncTempDispense(con_farmac,clinic_name)
#' 

getUsSyncTempDispense <- function(con.farmac, clinic.name) {
  
  
  sync_temp_dispense <- tryCatch({
    
    
    temp_dispenses  <- dbGetQuery( con.farmac , paste0("select * from sync_temp_dispense where sync_temp_dispenseid = '" , clinic.name, "' ;" )  )
    
    return(temp_dispenses)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getUsSyncTempDispense -> Busca Dispensas de uma det. US na tabela sync_temp_dispense no servidor FARMAC PosgreSQL ',
                 error = as.character(cond$message) )  
    
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
       saveLogError(us.name = clinic.name,
                    event.date = as.character(Sys.time()),
                    action = 'Warning getUsSyncTempDispense -> Busca Dispensas de uma det. US na tabela sync_temp_dispense no servidor FARMAC PosgreSQL ',
                    error = as.character(cond$message) )  
       
       return(FALSE)
       
     } else {
       
       if (exists('temp_dispenses')){
         
         return(temp_dispenses)
       }
       
       
     }
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  sync_temp_dispense
  
  
}

#' getLocalSyncTempDispense -> Busca Dispensas no postgres local na tabela sync_temp_dispense
#' esta tabela e usada para comparacao com os dados vindos do servidor da farmac para evitar duplicacoes
#' 
#' @param con.local  obejcto de conexao com BD iDART
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas  da US
#' @examples 
#' main.clinic.name <- 'CS Albazine'
#' con_farmac <- getLocalServerCon()
#' ua_temp_sync_dispense<- getLocalSyncTempDispense(con_farmac,main.clinic.name)
#' 

getLocalSyncTempDispense <- function(con.local) {
  
  
  sync_temp_dispense <- tryCatch({
    
    
    sync_temp_dispense  <- dbGetQuery( con.local , paste0("select * from sync_temp_dispense ;" )  )
    return(sync_temp_dispense)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.time()),
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
#' @param con_postgres  obejcto de conexao com BD
#' @param df.dispenses o datafrane com as dispensas dos pacientes referidos (apenas as novas)  
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- sendDispenseToServer(con_farmac,pacientes_referidos)
#TODO addicionar bloco de warnings no trycatch

sendDispenseToServer <- function(con_postgres ,df.dispenses ){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    status_send <- dbWriteTable(con_postgres, "sync_temp_dispense", df.dispenses, row.names=FALSE, append=TRUE)
    
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
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = ' sendDispenseToServer ->  Erro ao inserir as  dispensas dos pacientes da farmac para o servidor local ',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = main_clinic_name ,
                   event.date = as.character(Sys.time()),
                   action = ' sendDispenseToServer ->  Erro ao enviar dispensas dos pacientes da farmac para o  Servidor Farmac ',
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
        # guardar o log 
        saveLogError(us.name = farmac_name ,
                     event.date = as.character(Sys.time()),
                     action = 'Warning  sendDispenseToServer -> Envia dispensas dos pacientes da farmac para o  Servidor Local  ',
                     error = as.character(cond$message) )  
        
      } else {
        
        # guardar o log 
        saveLogError(us.name = main_clinic_name,
                     event.date = as.character(Sys.time()),
                     action = 'Warning  sendDispenseToServer -> Envia dispensas dos pacientes da farmac para o  Servidor Farmac  ',
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



#' getDispensesToSendOpenMRS -> Busca Dispensas na tabela sync_temp_dispense para  actualizar em prescription , packagedrugs, package e OpenMRS
#' no servidor Local
#' 
#' @param con.postgres   obejcto de conexao com BD iDART
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas que ainda nao forma enviadas para openmrs
#' @examples 
#' con_local <- getFarmacServerCon()
#' dispensesToSend <- getFarmacSyncTempDispenseByName(con_farmac,farmac.name)
#' 

getDispensesToSendOpenMRS <- function(con.local) {
  
  
  sync_temp_dispense <- tryCatch({
    
    
    sync_temp_dispense  <- dbGetQuery( con.local , paste0( "select * from sync_temp_dispense where ( imported is null or imported ='' ) and sync_temp_dispenseid ='",main_clinic_name , "' ; ")   )
    return(sync_temp_dispense)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.time()),
                 action = 'getDispensesToSendOpenMRS -> Busca Dispensas na tabela sync_temp_dispense para  actualizar em prescription , packagedrugs, package e OpenMRS ',
                 error = as.character(cond$message) )  
    
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
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = 'warning getDispensesToSendOpenMRS -> Busca Dispensas na tabela sync_temp_dispense para  actualizar em prescription , packagedrugs, package e OpenMRS ',
                   error = as.character(cond$message) )  
      
      return(FALSE)
      
    } else {
      
      if (exists('sync_temp_dispense')){
        
        return(sync_temp_dispense)
      }
      
      
    }
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  sync_temp_dispense
  
  
}


#' saveNewPrescription  -> salva uma nova  prescricao na BD
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df.prescription o datafrane com a prescricao nova
#' @examples 
#' 
#' status <- saveNewPrescription(con_local,prescription)
#TODO addicionar bloco de warnings no trycatch

saveNewPrescription <- function(con_postgres ,df.prescription ){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    status_send <- dbWriteTable(con_postgres, "prescription", df.prescription, row.names=FALSE, append=TRUE)
    
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
                   action = 'saveNewPrescription  -> salva uma nova  prescricao na BD ',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = ' saveNewPrescription  -> salva uma nova  prescricao na BD',
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
        # guardar o log 
        saveLogError(us.name =  farmac_name ,
                     event.date = as.character(Sys.time()),
                     action = 'Warning saveNewPrescription  -> salva uma nova  prescricao na BD',
                     error = as.character(cond$message) )  
        
      } else {
        
        # guardar o log 
        saveLogError(us.name = main_clinic_name,
                     event.date = as.character(Sys.time()),
                     action = 'Warning  saveNewPrescription  -> salva uma nova  prescricao na BD  ',
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



#' saveNewPrescribedDrug  -> salva uma nova  PrescribedDrug na BD
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df.prescribed.drug o datafrane com a prescricao nova
#' @examples 
#' 
#' status <- saveNewPrescription(con_local,prescription)
#TODO addicionar bloco de warnings no trycatch

saveNewPrescribedDrug <- function(con_postgres ,df.prescribed.drug ){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    status_send <- dbWriteTable(con_postgres, "prescribeddrugs", df.prescribed.drug, row.names=FALSE, append=TRUE)
    
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
                   action = 'saveNewPrescribedDrug  -> salva uma nova  PrescribedDrug na BD',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = ' saveNewPrescribedDrug  ->salva uma nova  PrescribedDrug na BD',
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
        # guardar o log 
        saveLogError(us.name = farmac_name ,
                     event.date = as.character(Sys.time()),
                     action = 'Warning saveNewPrescribedDrug  -> salva uma nova  PrescribedDrug na BD',
                     error = as.character(cond$message) )  
        
      } else {
        
        # guardar o log 
        saveLogError(us.name = main_clinic_name,
                     event.date = as.character(Sys.time()),
                     action = 'Warning  saveNewPrescribedDrug  -> salva uma nova  PrescribedDrug na BD ',
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



#' getPrescriptionFromPatient -> Busca a prescricao de  paciente
#' 
#' @param con.local   obejcto de conexao com BD iDART
#' @param con.local   data do levantamento
#' @return prescription df 
#' @examples 
#' con_local <- getFarmacServerCon()
#' prescriptipn <- getPrescriptionFromPatient(con_farmac,nid)
#' 

getPrescriptionFromPatient <- function(con.local, date.pickup,patient.id) {
  
  
  prescription <- tryCatch({
    
    
    tmp_prescription  <- dbGetQuery( con.local ,
                                     paste0(  "select * from prescription   where prescription.date = '", date.pickup, "' AND patient = ", as.numeric(patient.id)  ) )
    
    if(nrow(tmp_prescription)>1){ #  se tiver mais de uma prescricao na mesma data consdererar a ultima
      tmp_prescription <- tmp_prescription[nrow(tmp_prescription),]
    }
    
    return(tmp_prescription)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = main_clinic_name,
                 event.date = as.character(Sys.time()),
                 action = ' getPrescriptionFromPatient -> Busca a prescricao de  paciente ',
                 error = as.character(cond$message) )  
    
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
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = 'warning  getPrescriptionFromPatient -> Busca a prescricao de  paciente',
                   error = as.character(cond$message) )  
      
      return(FALSE)
      
    } else {
      
      if (exists('tmp_prescription')){
        
        return(tmp_prescription)
      }
      
      
    }
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  prescription
  
  
}

