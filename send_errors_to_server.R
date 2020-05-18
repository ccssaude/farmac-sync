###########################################################

# Limpar o envinronment & inicializar as

rm(list = setdiff(ls(), c("wd", "is.farmac")))

source('config/config_properties.R')


###########################################################

# con_farmac = FALSE para casos de conexao nao estabelecida
if (!is.logical(con_farmac)) {
  # Get local  connections
  # con_local = FALSE para casos de conexao nao estabelecida
  if (!is.logical(con_local)) {
    if (is.farmac) {
      log_error_server <- getLogErrorFromServer(con_farmac, farmac_name)
      
    }
    else {
      log_error_server <-
        getLogErrorFromServer(con_farmac, main_clinic_name)
      
    }
    
    if (!is.logical(log_error_server)) {
      if (nrow(log_error_server) == 0) {
        if (nrow(logErro) == 0) {
          if (is.farmac) {
            message(paste0(farmac_name, ' - sem logs por enviar.'))
          } else{
            message(paste0(main_clinic_name, ' - sem logs por enviar.'))
          }
          
          
        }
        else {
          log_error_to_send <- logErro
          status <-
            sendLogError(con_postgres = con_farmac, df.logerror =  log_error_to_send)
          if (!status) {
            # salvar o ficheiro dos logs das dispensas
            save(logErro, file = 'logs/logErro.RData')
          }
          
        }
        
      }
      else {
        if (nrow(logErro) == 0) {
          if (is.farmac) {
            message(paste0(farmac_name, ' - sem logs por enviar.'))
          } else{
            message(paste0(main_clinic_name, ' - sem logs por enviar.'))
          }
          
        }
        else {
          log_error_to_send <-
            anti_join(logErro,
                      log_error_server,
                      by = c('data_evento', 'erro'))
          if (nrow(log_error_to_send) > 0) {
            status <-
              sendLogError(con_postgres = con_farmac, df.logerror = log_error_to_send)
            
            if (!status) {
              # salvar o ficheiro dos logs das dispensas
              save(logErro, file = 'logs/logDispensa.RData')
              
            } else{
              message('Log erros enviados com sucesso')
              
            }
            
          } else{
            if (is.farmac) {
              message(paste0(farmac_name, ' - sem logs por enviar.'))
            } else{
              message(paste0(main_clinic_name, ' - sem logs por enviar.'))
            }
          }
          
          
        }
        
        
      }
      
      
      
    }   else {
      save(logErro, file = 'logs/logErro.RData')
    }
    
  }
  else {
    # erro de comunicacao
    save(logErro, file = 'logs/logErro.RData')
  }
  
  
  
  
} else {
  # erro de comunicacao
  save(logErro, file = 'logs/logErro.RData')
}
