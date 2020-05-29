
library(dplyr)

#' insertPatient -> insere pacientes na tabela patient
#' 
#' @param con.postgres  obejcto de conexao com BD iDART
#' @param df.patients informacao dos pacientes (accountstatus, cellphone, dateofbirth, clinic, firstnames, homephone, lastname, modified, patientid, province, sex, workphone, address1, address2, address3, nextofkinname, nextofkinphone, race, uuid, uuidopenmrs)
#' @return TRUE/FALSE
#' @examples 
#' 
#' 
insertPatient <- function(con.postgres,df.patients){
  
  # 
  # base_query = " INSERT INTO public.patient(accountstatus, cellphone, dateofbirth, clinic, firstnames, homephone, lastname, modified, patientid, province, sex, workphone, address1, address2, address3, nextofkinname, nextofkinphone, race, uuid, uuidopenmrs)
  # VALUES ( " 
  # 
  # insert_query = paste0(base_query,string_values ," ;")
  # 
  
  status <- tryCatch({
    
    
    status = dbWriteTable(con.postgres, "patient", df.patients ,append=TRUE, row.names=FALSE)
    
    return(status)
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.time()),
                 action = 'insertPatient -> insere pacientes na tabela patient ',
                 error = as.character(cond$message) )  
    
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

#' insertPatientIdentifier -> insere pacientes na tabela PatientIdentifier 
#' 
#' @param con.postgres  obejcto de conexao com BD iDART
#' @param df.patientidentifier informacao dos pacientes (accountstatus, cellphone, dateofbirth, clinic, firstnames, homephone, lastname, modified, patientid, province, sex, workphone, address1, address2, address3, nextofkinname, nextofkinphone, race, uuid, uuidopenmrs)
#' @return TRUE/FALSE
#' @examples 
#' 
#' 
insertPatientIdentifier <- function(con.postgres,df.patientidentifier){
  
  
  status <- tryCatch({
    
    
    status = dbWriteTable(con.postgres, "patientidentifier", df.patientidentifier ,append=TRUE, row.names=FALSE)
    
    return(status)
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.time()),
                 action = 'insertPatientIdentifier -> insere pacientes na tabela patientidentifier ',
                 error = as.character(cond$message) )  
    
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


#' insertPatientAttribute -> insere data inicio tarv tabela PatientAttribute 
#' 
#' @param con.postgres  obejcto de conexao com BD iDART
#' @param df.patientattribute informacao dos pacientes (accountstatus, cellphone, dateofbirth, clinic, firstnames, homephone, lastname, modified, patientid, province, sex, workphone, address1, address2, address3, nextofkinname, nextofkinphone, race, uuid, uuidopenmrs)
#' @return TRUE/FALSE
#' @examples 
#' 
#' 
insertPatientAttribute <- function(con.postgres,df.patientattribute){
  
  status <- tryCatch({
    
    
    status = dbWriteTable(con.postgres, "patientattribute", df.patientattribute ,append=TRUE, row.names=FALSE)
    
    return(status)
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.time()),
                 action = 'insertPatientAttribute -> insere pacientes na tabela patientatribute ',
                 error = as.character(cond$message) )  
    
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


#' receberPacienteServer -> carrega pacientes referidos para farmac
#'
#' @param con.postgres  obejcto de conexao com BD iDART
#' @param df.patientidentifier informacao dos pacientes (accountstatus, cellphone, dateofbirth, clinic, firstnames, homephone, lastname, modified, patientid, province, sex, workphone, address1, address2, address3, nextofkinname, nextofkinphone, race, uuid, uuidopenmrs)
#' @return TRUE/FALSE
#' @examples
#'
#'
receberPacienteServer <- function(con.postgres,df.pacientesporreferir){
  
  
  status <- tryCatch({
    
    
    status = dbWriteTable(con.postgres, "sync_temp_patients", df.pacientesporreferir ,append=TRUE, row.names=FALSE)
    
    return(status)
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.time()),
                 action = 'insertPatientIdentifier -> insere pacientes na tabela patientidentifier ',
                 error = as.character(cond$message) )  
    
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




#' referirPacientes -> envia pacientesreferidos  da US para o servidor 
#' 
#' @param con.postgres  obejcto de conexao com BD iDART
#' @param df.patientidentifier informacao dos pacientes (accountstatus, cellphone, dateofbirth, clinic, firstnames, homephone, lastname, modified, patientid, province, sex, workphone, address1, address2, address3, nextofkinname, nextofkinphone, race, uuid, uuidopenmrs)
#' @return TRUE/FALSE
#' @examples 
#' 
#' 
referirPacientes <- function(con.postgres,df.pacientesporreferir){
  
  
  status <- tryCatch({
    
    
    status = dbWriteTable(con.postgres, "sync_temp_patients", df.pacientesporreferir ,append=TRUE, row.names=FALSE)
    
    return(status)
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = farmac_name,
                 event.date = as.character(Sys.time()),
                 action = 'insertPatientIdentifier -> insere pacientes na tabela patientidentifier ',
                 error = as.character(cond$message) )  
    
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