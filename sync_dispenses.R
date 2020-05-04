library(RPostgreSQL)
library(dplyr)
library(plyr)

# carrega funcoes
source('Functions/generic_functions.R')
source('config/config_properties.R')
source('Functions/dispense_functions.R')

# Load logs df
load('logs/logErro.RData')
load('logs/logDispensa.RData') 


# Get farmac  connections
con_farmac <- getFarmacServerCon()

# con_farmac = FALSE para casos de conexao nao estabelecida
if(! is.logical(con_farmac) ){
  
  # Get local  connections
  con_local <- getLocalServerCon()
  
  # con_local = FALSE para casos de conexao nao estabelecida
  if( ! is.logical(con_local) ) {
    
    
    # get local dispenses
    sync_dispense_local <- getLocalSyncTempDispense(con_local )
    
    # sync_dispense_local = FALSE para casos em que nao foi possivl buscar as dispensas (falha de rede,etc )
    if(! is.logical(sync_dispense_local)){
      
      # add uma coluna com o nome da farmac - para ser facil identificar a provenciencia do registo no servidor da FARMAC
      sync_dispense_local$clinic_name_farmac <- farmac_name
      
      # Busca dispensas do servidor  farmac ( apenas da farmac_name) 
      sync_dispense_farmac <- getFarmacSyncTempDispense(  con_farmac, farmac_name )
      
      if( ! is.logical(sync_dispense_farmac)){
        
        
        ## filtrar apenas novas dispensas 
          dispenses_to_send <- subset(sync_dispense_local, ! sync_dispense_local$id %in% sync_dispense_farmac$id ,) 
          
          if(nrow(dispenses_to_send) > 0){
            
            # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
            status <- sendDispenseToServer(con.farmac = con_farmac,df.dispenses = dispenses_to_send)
            
            if(status){
              
              # escrever no log todos dispensas enviadas
              for (i in 1:dim(dispenses_to_send)[1] ) {
                
                data <- as.character(Sys.Date())
                patient   <-  gsub(pattern = 'NA',
                                   replacement = ' ',
                                   x =  paste0( dispenses_to_send$patientid[i],' ', dispenses_to_send$patientfirstname[i], ' ', dispenses_to_send$patientlastname[i] ) )
                
                data_levantamento  <- dispenses_to_send$dispensedate[i]
                
                saveLogDispensa(farmac.name = farmac_name,event.date = data,patient = patient,dispense.date = as.character(data_levantamento))
                
                
              }
              
              
              ## TODO
              ## Enviar email (farmaciamaputo@ccsaude.org.mz) com um xls em anexo do df dispensas enviadas
              
              # salvar o ficheiro dos logs dos pacientes referidos
              save(log_dispensas,file = 'logs/logDispensa.RData')
            } else {
              ##  A exception sera capturada na funcao sendDispenseToServer
              save(logErro,file = 'logs/logErro.RData')
            }
            
          } else{
            
            # Do nothing
            # nao ha novas dispensas para enviar
            message('Nao ha dispensas novas por enviar')
          }
          
          
          
         
        
      } else {
        
        ## TODO
        ## Houve problema de conexao...
        ## gravar os logs
        ## programar uma funcao  para enviar os logs novos (com base na data)
        ##para o servidor com os dados na df logsErro (uma vez semana)
        save(logErro,file = 'logs/logErro.RData')
        
      }
      
    } else {
      
      ## TODO
      ## Houve problema de conexao...
      ## gravar os logs
      ## programar uma funcao  para enviar os logs novos (com base na data)
      ##para o servidor com os dados na df logsErro (uma vez semana)
      save(logErro,file = 'logs/logErro.RData')
      
    }
    
  } else {
    
    ## TODO
    ## Houve problema de conexao...
    ## gravar os logs
    ## programar uma funcao  para enviar os logs novos (com base na data)
    ##para o servidor com os dados na df logsErro (uma vez semana)
    save(logErro,file = 'logs/logErro.RData')
    
  }

} else {
  
  ## TODO
  ## Houve problema de conexao...
  ## gravar os logs
  ## programar uma funcao  para enviar os logs novos (com base na data)
  ##para o servidor com os dados na df logsErro (uma vez semana)
  save(logErro,file = 'logs/logErro.RData')
  
}