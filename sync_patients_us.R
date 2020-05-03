library(RPostgreSQL)
library(dplyr)
library(plyr)

# carrega funcoes
source('Functions/generic_functions.R')
source('Functions/patient_functions.R')


# Load logs df
load('logs/logErro.RData')
load('logs/logReferencia.RData')

# Get connections
con_farmac <- getFarmacServerCon()
con_local <- getLocalServerCon()



sync_patients_local <- getLocalSyncTempPatients(con.local = con_local,main.clinic.name = main_clinic_name )
sync_patients_farmac <- getFarmacSyncTempPatients(con.farmac =  con_farmac,main.clinic.name = main_clinic_name )


if(exists('sync_patients_local') && exists('sync_patients_farmac')){
  
  ## filtrar apenas pacientes novos
  patients_to_send <- subset(sync_patients_local, !sync_patients_local$patientid %in% sync_patients_farmac$patientid ,) 
  if(dim(patients_to_send)[1] > 0){
    
    # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
    status <- refferPatients(con.farmac = con_farmac,reffered.patients = patients_to_send)
    
    if(status){
      
      # escrever no log todos de referencia
      for (i in 1:dim(patients_to_send)[1] ) {
        
        us_name <- main_clinic_name
        data <- as.character(Sys.time())
        patient   <-  gsub(pattern = 'NA',
                           replacement = ' ',
                           x =  paste0( patients_to_send$patientid[i],' ', patients_to_send$firstnames[i], ' ', patients_to_send$lastname[i] ) )
        
        us_ref  <- patients_to_send$clinicname[i]
        
        saveLogReferencia(us_name,data,patient ,us_ref)
        
        
      }
      
      
      ## TODO
      ## Enviar email (farmaciamaputo@ccsaude.org.mz) com um xls em anexo do df LogReferencia
      
      # salvar o ficheiro dos logs dos pacientes referidos
      save(logReferencia,file = 'logs/logReferencia.RData')
    } else {
      ##  A exception sera capturada na funcaorefferPatients
      save(logErro,file = 'logs/logErro.RData')
    }
    
  } else{
    
    # Do nothing
    # nao ha pacientes novos
    message('Nao ha pacientes novos referidos')
  }
  
  
  
} else {
  
  save(logErro,file = 'logs/logErro.RData')
  ## TODO
  ## Houve problema de conexao...
  ## gravar os logs
  ## programar uma funcao  para enviar os logs novos (com base na data)
  ##para o servidor com os dados na df logsErro (uma vez semana)
  
}