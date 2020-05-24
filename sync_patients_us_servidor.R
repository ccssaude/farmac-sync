########################################################### 

# Limpar o envinronment & inicializar as 

rm(list=setdiff(ls(), c("wd", "is.farmac") ))

source('Functions/patient_functions.R') 
########################################################### 

# verifica se as conexoes foram criadas com sucesso is.logical(con_farmac) =FALSE

if(!is.logical(con_farmac) & !is.logical(con_local)){
  
  #query que busca todos pacientes da unidade sanitaria
  sql_patient_local = 'select* from sync_temp_patients'
  
  #query que busca todos pacientes do servidor filtrando apenas os pacientes da Unidade sanitaria em causa
  sql_patient_server = paste0( "select * from sync_temp_patients where mainclinicname ='", main_clinic_name, "' ;")
  
  ##todos pacientes da unidade sanitaria
  patient_local = dbGetQuery(con_local, sql_patient_local)
  
  ##todos pacientes do servidor
  patient_server = dbGetQuery(con_farmac, sql_patient_server)
  
  if(exists('patient_local') && exists('patient_server')){
    
    #este datafrane busca todos pacientes que estao nao unidade sanitaria mas nao estao no servidor central
    novos_patients_por_referir = anti_join(patient_local, patient_server, by="patientid")
    
    #esta linha de codigo, converte o campo datebirth para um formato reconhecido pela maquina
    #novos_patients_por_referir$dateofbirth=format(novos_patients_por_referir$dateofbirth, "%Y-%m-%d %H:%M:%S")
    
    #por fim esta linha manda apenas os pacientes referidos para o servidor central
    stutus = dbWriteTable(con_farmac, 'sync_temp_patients', novos_patients_por_referir, append=TRUE, row.names=FALSE)
    if(status){
      
      message(paste0(nrow(novos_patients_por_referir), " Foram enviados ao sevidor"))
    } else{
      
      save(logErro,file = 'logs/logErro.RData')
      
      message( "Tente novamente.Eerro ao enviar pacientes ao servidor ... Houve problema de conexao..." )
    }
    
    #main_clinic_name = unidade_sanitaria
    
  } else {
    
    save(logErro,file = 'logs/logErro.RData')
    
    message( "Houve problema de conexao..." )
    ## gravar os logs
    ## programar uma funcao  para enviar os logs novos (com base na data)

  }
  
  

  
}else{
  
  save(logErro,file = 'logs/logErro.RData')
  
  message( "Houve problema de conexao..." )
  ## gravar os logs
  ## programar uma funcao  para enviar os logs novos (com base na data)
  
}