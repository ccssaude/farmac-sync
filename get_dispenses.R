########################################################### 

# Limpar o envinronment & inicializar as 

rm(list=setdiff(ls(), c("wd", "is.farmac") ))

source('config/config_properties.R')     


########################################################### 

# con_farmac = FALSE para casos de conexao nao estabelecida
if(! is.logical(con_farmac) ){
  
  # Get local  connections
  
  # con_local = FALSE para casos de conexao nao estabelecida
  if( ! is.logical(con_local) ) {
    
    
    # get local dispenses
    sync_dispense_local <- getLocalSyncTempDispense(con_local )
  
    # sync_dispense_local = FALSE para casos em que nao foi possivl buscar as dispensas (falha de rede,etc )
    if(! is.logical(sync_dispense_local)){
      
      
      # Busca dispensas do servidor  farmac ( apenas da US) 
      sync_dispense_farmac <- getUsSyncTempDispense( con_farmac, main_clinic_name )
      
      #sync_dispense_farmac <- getUsSyncTempDispense( con_farmac, "main_clinic_name" )
      if( ! is.logical(sync_dispense_farmac)){
  
        ## filtrar apenas novas dispensas enviadas pelas farmacs
        
        if(nrow(sync_dispense_farmac)== 0 ){ # get all dispenseses
          
          message("Sem dispensas para actualizar")
          
        }
        else{
          
          if(nrow(sync_dispense_local) > 0){
            # remover a coluna imported ( nao precisamos dela neste script)
            sync_dispense_farmac <- sync_dispense_farmac[ , -which( names(sync_dispense_farmac) %in% c("imported"))]
            sync_dispense_local <- sync_dispense_local[ , -which( names(sync_dispense_local) %in% c("imported"))]
            sync_dispense_local <- sync_dispense_local[ , -which( names(sync_dispense_local) %in% c("openmrs_status"))]
            sync_dispense_local <- sync_dispense_local[ , -which( names(sync_dispense_local) %in% c("send_openmrs"))]
            
            
           # dispenses_to_get <-  anti_join(sync_dispense_farmac, sync_dispense_local,   by=c('id','clinic_name_farmac') )
            dispenses_to_get <-  anti_join(sync_dispense_farmac, sync_dispense_local,   by=c('id') )
            if(nrow(dispenses_to_get) > 0){
              
              # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
              status <- sendDispenseToServer(con_postgres  = con_local,df.dispenses = dispenses_to_get)
              
              if(status){
                
                # escrever no log todos dispensas recebidas
                for (i in 1:dim(dispenses_to_get)[1] ) {
                  
                  data <- as.character(Sys.time())
                  patient   <-  gsub(pattern = 'NA',
                                     replacement = ' ',
                                     x =  paste0( dispenses_to_get$patientid[i],' ', dispenses_to_get$patientfirstname[i], ' ', dispenses_to_get$patientlastname[i] ) )
                  
                  data_levantamento  <- substr(dispenses_to_get$dispensedate[i],start = 1,stop = 10)
                  
                  saveLogDispensa(clinic_name = main_clinic_name,event.date = data,patient = patient,dispense.date = as.character(data_levantamento))
                  
                  
                }
                message(paste0('Actualizadas ', nrow(dispenses_to_get), ' dispensas da farmac'))
                
                log_farmac_dispenses <- getLogDispenseFromServer(con.farmac = con_farmac,
                                                                 clinic.name = main_clinic_name)
                
                if(! is.logical(log_farmac_dispenses)){ 
                  
                  if(nrow(log_farmac_dispenses)==0){
                    
                    if(nrow(log_dispensas)==0){
                      
                      message(paste0(main_clinic_name,' - sem logs por enviar.'))
                      
                    } else {
                      
                      log_dispenses_to_send <- log_dispensas
                      status <- sendLogDispense(con_postgres = con_farmac, df.logdispense =  log_dispenses_to_send)
                      
                      if(status){
                        # salvar o ficheiro dos logs das dispensas
                        save(log_dispensas,file = 'logs/logDispensa.RData')
                        
                      } else{
                        # salvar o ficheiro logs das dispensas & dos erros
                        save(log_dispensas,file = 'logs/logDispensa.RData')
                        save(logErro, file = 'logs/logErro.RData')
                        
                      }
                      
                      
                    
                  }
                    
                    } else {
                    
                    if(nrow(log_dispensas)==0){
                      
                      log_dispensas <- log_farmac_dispenses
                      save(log_dispensas,file = 'logs/logDispensa.RData')
                    } else {
                      
                      
                     log_dispenses_to_send <- anti_join( log_dispensas,log_farmac_dispenses,  by=c('paciente','data_levantamento'))
                     if(nrow(log_dispenses_to_send)>0 ){
                       status <- sendLogDispense(con_postgres = con_farmac,df.logdispense = log_dispenses_to_send)
                       
                       if(status){
                         # salvar o ficheiro dos logs das dispensas
                         save(log_dispensas,file = 'logs/logDispensa.RData')
                         
                       } else{
                         # salvar o ficheiro logs das dispensas & dos erros
                         save(log_dispensas,file = 'logs/logDispensa.RData')
                         save(logErro, file = 'logs/logErro.RData')
                         
                       }
                       
                     } else{
                       message(paste0(main_clinic_name, ' - Sem logs novos para enviar'))
                     }
                     
                      
                    }
                    
                    
                  }
                  
                  
                  
                }
                else {
                  ##  A exception sera capturada na funcao getLogDispenseFromServer
                  # salvar o ficheiro dos logs das dispensas
                  save(log_dispensas,file = 'logs/logDispensa.RData')
                  save(logErro,file='logs/logErro.RData')
                }
                
                
                
                
                
              } else {
                ##  A exception sera capturada na funcao sendDispenseToServer
                save(logErro,file = 'logs/logErro.RData')
              }
              
              source(file = 'send_errors_to_server.R')
              
            } else{
              
              # Do nothing
              # nao ha novas dispensas para actualizar
              message('Nao ha dispensas novas por actualizar')
            }
            
          }
          else{ # primeiro envio
            
            # nao ha dispensas no servidor  local -> buscar todas do servidor farmac
            dispenses_to_get <- sync_dispense_farmac
            dispenses_to_get <- dispenses_to_get[ , -which( names(dispenses_to_get) %in% c("imported"))]
            
            # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
            status <- sendDispenseToServer(con_postgres  = con_local,df.dispenses = dispenses_to_get)
            
            if(status){
              
              # escrever no log todos dispensas recebidas hahaha
              for (i in 1:dim(dispenses_to_get)[1] ) {
                
                data <- as.character(Sys.time())
                patient   <-  gsub(pattern = 'NA',
                                   replacement = ' ',
                                   x =  paste0( dispenses_to_get$patientid[i],' ', dispenses_to_get$patientfirstname[i], ' ', dispenses_to_get$patientlastname[i] ) )
                
                data_levantamento  <- substr(dispenses_to_get$dispensedate[i],start = 1,stop = 10)
                
                saveLogDispensa(clinic_name = main_clinic_name,event.date = data,patient = patient,dispense.date = as.character(data_levantamento))
                
                
              }
              message(paste0('Actualizadas ', nrow(dispenses_to_get), ' dispensas '))
              
              log_farmac_dispenses <- getLogDispenseFromServer(con.farmac = con_farmac,
                                                               clinic.name = main_clinic_name)
              
              if(! is.logical(log_farmac_dispenses)){ 
                
                if(nrow(log_farmac_dispenses)==0){
                  
                  if(nrow(log_dispensas)==0){
                    
                    message(paste0(main_clinic_name,' - sem logs por enviar.'))
                  }
                  else {
                    log_dispenses_to_send <- log_dispensas
                    status <- sendLogDispense(con_postgres = con_farmac,df.logdispense =  log_dispenses_to_send)
                    
                    if(status){
                      # salvar o ficheiro dos logs das dispensas
                      save(log_dispensas,file = 'logs/logDispensa.RData')
                      
                    } else{
                      # salvar o ficheiro logs das dispensas & dos erros
                      save(log_dispensas,file = 'logs/logDispensa.RData')
                      save(logErro, file = 'logs/logErro.RData')
                      
                    }
                    
                    
                  }
                  
                  } 
                else {
                  
                  if(nrow(log_dispensas)==0){
                    
                    message(paste0(main_clinic_name,' - sem logs por enviar.'))
                  } 
                  else{
                    
                    log_dispenses_to_send <- anti_join(log_dispensas,log_farmac_dispenses,  by=c('paciente','data_levantamento'))
                    status <- sendLogDispense(con_postgres = con_farmac,df.logdispense =  log_dispenses_to_send)
                    
                    if(status){
                      # salvar o ficheiro dos logs das dispensas
                      save(log_dispensas,file = 'logs/logDispensa.RData')
                      
                    } else{
                      # salvar o ficheiro logs das dispensas & dos erros
                      save(log_dispensas,file = 'logs/logDispensa.RData')
                      save(logErro, file = 'logs/logErro.RData')
                      
                    }
                    
                    
                  }

                  
                    
                  }
                
                
                
              } else {
                ##  A exception sera capturada na funcao getLogDispenseFromServer
                save(logErro,file = 'logs/logErro.RData')
              }
              
              
              
              
              } else {
              ##  A exception sera capturada na funcao sendDispenseToServer
              save(logErro,file = 'logs/logErro.RData')
            }
           
        }
          
        }

      }
      else {
        
       
        ## Houve problema de conexao...
        ## gravar os logs
        save(logErro,file = 'logs/logErro.RData')
        
      }
      
    }
    else {
      
      ## Houve problema de conexao...
      ## gravar os logs
  
      save(logErro,file = 'logs/logErro.RData')
      
    }
    
  }
  else {
    
    ## Houve problema de conexao...
    ## gravar os logs
    save(logErro,file = 'logs/logErro.RData')
    
  }
  
} else {
  
  ## Houve problema de conexao...
  ## gravar os logs
  save(logErro,file = 'logs/logErro.RData')
  message("Nao foi possivel connectar-se ao servidor farmac, veja os erros na console")
  
}

########################################################### 
