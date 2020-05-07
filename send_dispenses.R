
# Limpar o envinronment & inicializar as 

rm(list=setdiff(ls(), c("wd", "is.farmac") ))

source('config/config_properties.R')     


#####################################################################################################


# con_farmac = FALSE para casos de conexao nao estabelecida
if(! is.logical(con_farmac) ){
  
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
      
      if(! is.logical(sync_dispense_farmac)){
        
         if(nrow(sync_dispense_farmac) > 0){
           
           ## filtrar apenas novas dispensas 
           if(nrow(sync_dispense_local)> 0 ){
             # remover a coluna imported ( nao precisamos dela neste script)
             sync_dispense_farmac <- sync_dispense_farmac[ , -which( names(sync_dispense_farmac) %in% c("imported"))]
             dispenses_to_send <- anti_join(sync_dispense_local ,sync_dispense_farmac , by=c("id") )
             
             if(nrow(dispenses_to_send) > 0){
               
               # addiciona a coluna do nome da farmac para sabarmos qual farmac enviou
               dispenses_to_send$clinic_name_farmac <- farmac_name
               # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
               status <- sendDispenseToServer(con_postgres =  con_farmac,df.dispenses = dispenses_to_send)
               
               if(status){
                 
                 # escrever no log todos dispensas enviadas
                 for (i in 1:dim(dispenses_to_send)[1] ) {
                   
                   data <- as.character(Sys.time())
                   patient   <-  gsub(pattern = 'NA',
                                      replacement = ' ',
                                      x =  paste0( dispenses_to_send$patientid[i],' ', dispenses_to_send$patientfirstname[i], ' ', dispenses_to_send$patientlastname[i] ) )
                   
                   data_levantamento  <- substr(dispenses_to_send$dispensedate[i],start = 1,stop = 10)
                   
                   saveLogDispensa(clinic_name =  farmac_name,event.date = data,patient = patient,dispense.date = as.character(data_levantamento))
                   
                   
                 }
                 

                 log_farmac_dispenses <- getLogDispenseFromServer(con.farmac = con_farmac,
                                                                  clinic.name = farmac_name)
                 
                 if(! is.logical(log_farmac_dispenses)){ 
                   
                   if(nrow(log_farmac_dispenses)==0){
                     
                     if(nrow(log_dispensas)==0){
                       
                       message(paste0(main_clinic_name,' - sem logs por enviar.'))
                     }
                     else {
                       log_dispenses_to_send <- log_dispensas
                       status <- sendDispenseToServer(con_postgres = con_farmac,df.dispenses = log_dispenses_to_send)
                       
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
                     } else{
                       
                       log_dispenses_to_send <- anti_join(log_dispensas,log_farmac_dispenses,  by=c('paciente','data_levantamento'))
                       
                       if(nrow(log_dispenses_to_send)>0){
                         
                         status <- sendLogDispense(con_postgres = con_farmac,df.logdispense =  log_dispenses_to_send)
                         
                         if(status){
                           # salvar o ficheiro dos logs das dispensas
                           save(log_dispensas,file = 'logs/logDispensa.RData')
                           
                         } else{
                           # salvar o ficheiro logs das dispensas & dos erros
                           save(log_dispensas,file = 'logs/logDispensa.RData')
                           save(logErro, file = 'logs/logErro.RData')
                           
                         }
                         
                       } else{
                         
                         message(paste0(farmac_name,' - Sem  logs de dispenas novas por enviar.'))
                         
                       }
                      
                       
                       
                     }
                     
                     
                     
                   }
                   
                   
                   
                 } else {
                   ##  A exception sera capturada na funcao getLogDispenseFromServer
                   save(logErro,file = 'logs/logErro.RData')
                   save(log_dispensas,file = 'logs/logDispensa.RData')
                 }
                 
                 
               } else {
                 ##  A exception sera capturada na funcao sendDispenseToServer
                 save(logErro,file = 'logs/logErro.RData')
               }
               
             } else {
               
               message(paste0(farmac_name,' - Sem dispensas novas por enviar'))
             }
             
           } else {
             
             message(paste0(farmac_name,' - Sem dispensas novas por enviar'))
           }
    
         
         } else{ # primeiro envio ao servidor
           
           if(nrow(sync_dispense_local)> 0){ 
             
             dispenses_to_send <- sync_dispense_local 
            
             if(nrow(dispenses_to_send) > 0){
               
               # addiciona a coluna do nome da farmac para sabarmos qual farmac enviou
               dispenses_to_send$clinic_name_farmac <- farmac_name
               # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
               status <- sendDispenseToServer(con_postgres =  con_farmac,df.dispenses = dispenses_to_send)
               
               if(status){
           
                 # escrever no log todos dispensas enviadas
                 for (i in 1:dim(dispenses_to_send)[1] ) {
                   
                   data <- as.character(Sys.time())
                   patient   <-  gsub(pattern = 'NA',
                                      replacement = ' ',
                                      x =  paste0( dispenses_to_send$patientid[i],' ', dispenses_to_send$patientfirstname[i], ' ', dispenses_to_send$patientlastname[i] ) )
                   
                   data_levantamento  <- substr(dispenses_to_send$dispensedate[i],start = 1,stop = 10)
                   
                   saveLogDispensa(clinic_name =  farmac_name,event.date = data,patient = patient,dispense.date = as.character(data_levantamento))
                   
                   
                 }
                 
                 log_farmac_dispenses <- getLogDispenseFromServer(con.farmac = con_farmac,
                                                                  clinic.name = farmac_name)
                 
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
                       if(nrow(log_dispenses_to_send)> 0){
                         status <- sendLogDispense(con_postgres = con_farmac,df.logdispense =  log_dispenses_to_send)
                         
                         if(status){
                           # salvar o ficheiro dos logs das dispensas
                           save(log_dispensas,file = 'logs/logDispensa.RData')
                           
                         } else{
                           # salvar o ficheiro logs das dispensas & dos erros
                           save(log_dispensas,file = 'logs/logDispensa.RData')
                           save(logErro, file = 'logs/logErro.RData')
                           
                         }
                         
                         
                       } else { message(paste0(farmac_name, ' - Sem logs  novos por enviar'))}
                       
                       
                     }
                     
                     
                     
                   }
                   
                   
                   
                 } else {
                   ##  A exception sera capturada na funcao getLogDispenseFromServer
                   save(logErro,file = 'logs/logErro.RData')
                   save(log_dispensas,file = 'logs/logDispensa.RData')
                 }
                 

               } else {
                 ##  A exception sera capturada na funcao sendDispenseToServer
                 save(logErro,file = 'logs/logErro.RData')
               }
               
             
           }else{
             message(paste0(farmac_name, '- sem dispensas novas para enviar'))
             
           }

           } else{
             
             message(paste0(farmac_name, '- sem dispensas novas para enviar'))
           }
         
         }
      } else {

        ## Houve problema de conexao...
        ## gravar os logs
        save(logErro,file = 'logs/logErro.RData')
        
      }
      
    } else {
      
      ## Houve problema de conexao...
      ## gravar os logs
    
      save(logErro,file = 'logs/logErro.RData')
      
    }
    
  } else {
    
    ## Houve problema de conexao...
    save(logErro,file = 'logs/logErro.RData')
    
  }

} else {
  
  ## Houve problema de conexao...
  ## gravar os logs
  save(logErro,file = 'logs/logErro.RData')
  
}

