########################################################### 

# Limpar o envinronment & inicializar as 

rm(list=setdiff(ls(), c("wd", "is.farmac") ))

source('Functions/patient_functions.R') 
########################################################### 

# verifica se as conexoes foram criadas com sucesso is.logical(con_farmac) =FALSE

if(!is.logical(con_farmac) & !is.logical(con_local)){
sql_pacientes_us1=paste0("select * from sync_temp_patients where clinicname ='", farmac_name, "' ;")
pacientes_referidos = dbGetQuery(con_farmac, sql_pacientes_us1)

sql_pacientes_farmac = ("select * from sync_temp_patients")
pacientes_farmac = dbGetQuery(con_local, sql_pacientes_farmac)


# sync_patients_local <- getLocalSyncTempPatients(con.local = con_local,main.clinic.name = main_clinic_name )
# sync_patients_farmac <- getFarmacSyncTempPatients(con.farmac =  con_farmac,main.clinic.name = main_clinic_name )

# se conseguiu buscar os pacientes do servidor continua caso contraio termina
if(exists('pacientes_farmac') && exists('pacientes_referidos')){
  
  ## filtrar apenas pacientes novos
  novos_pacientes_por_inserir_farmac = anti_join(pacientes_referidos, pacientes_farmac, by='patientid')
  
  # se tiver nvos pacientes
  if(dim(novos_pacientes_por_inserir_farmac)[1] > 0){
    
    # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
    novos_pacientes_por_inserir_farmac$imported=NULL
    stutus = dbWriteTable(con_local, 'sync_temp_patients', novos_pacientes_por_inserir_farmac, append=TRUE, row.names=FALSE)
    
    if(status){
      
      # escrever no log todos de referencia
      ##atualiza a coluna imported com o valor "imported"
      message(paste0(nrow(novos_pacientes_por_inserir_farmac), "Novos pacientes referidos inseridos com sucesso "))
      
      base_query= " update sync_temp_patients set imported = 'imported' where id in "
      string_id = "( "
      for (i in 1:nrow(novos_pacientes_por_inserir_farmac)){
        
        
        if(i==nrow(novos_pacientes_por_inserir_farmac)){
          
          string_id =  paste0(string_id, novos_pacientes_por_inserir_farmac$id[i], ")")
          
        } else {
          
          string_id =  paste0(string_id, novos_pacientes_por_inserir_farmac$id[i], ", ")
          
        }
        
        
      }
      
      # actualiza a coluna imported
      update_query = paste0(base_query,string_id , " ;")
      status =  dbExecute(con_farmac,update_query)
      
      # buscar os ultimos IDs das tabelas
      last_id <- dbGetQuery(con_local,'select id from patient order by id desc limit 1; ')
      last_id <- last_id$id
      
      last_id_pi <- dbGetQuery(con_local, 'select id from patientidentifier order by id desc limit 1;')
      last_id_pi <- last_id_pi$id
      
      last_id_pa <- dbGetQuery(con_local, 'select id from patientattribute order by id desc limit 1;')
      last_id_pa <- last_id_pa$id
      
      
      ##insere os dados da sync_temp_patients, na tabela patient, patient_identifier, patient_attribute
      for (i in 1:nrow(novos_pacientes_por_inserir_farmac)){
        id = last_id + 5
        accountstatus = novos_pacientes_por_inserir_farmac$accountstatus[i]
        cellphone =  novos_pacientes_por_inserir_farmac$cellphone[i] 
        if(cellphone==""){
          cellphone="''"
        }
        dateofbirth =  novos_pacientes_por_inserir_farmac$dateofbirth[i]
        #dateofbirth <-  as.POSIXct(dateofbirth)
        #dateofbirth = format(dateofbirth, "%Y-%m-%d %H:%M:%S")
        clinic = 2  #novos_pacientes_por_inserir_farmac$clinic[i]
        firstnames=  novos_pacientes_por_inserir_farmac$firstnames[i] 
        homephone =  novos_pacientes_por_inserir_farmac$homephone[i] 
        if(  homephone==""){
          homephone="''"
        }
        lastname  = novos_pacientes_por_inserir_farmac$lastname[i] 
        modified =  novos_pacientes_por_inserir_farmac$modified[i]
        patientid =  novos_pacientes_por_inserir_farmac$patientid[i] 
        province =  novos_pacientes_por_inserir_farmac$province[i]
        sex =  novos_pacientes_por_inserir_farmac$sex[i]
        workphone =  novos_pacientes_por_inserir_farmac$workphone[i]
        address1=  novos_pacientes_por_inserir_farmac$address1[i]
        address2 =  novos_pacientes_por_inserir_farmac$address2[i]
        address3 = novos_pacientes_por_inserir_farmac$address3[i]
        nextofkinname =  novos_pacientes_por_inserir_farmac$nextofkinname[i]
        nextofkinphone =  novos_pacientes_por_inserir_farmac$nextofkinphone[i]
        race=  novos_pacientes_por_inserir_farmac$race[i]
        uuid = novos_pacientes_por_inserir_farmac$uuid[i]
        
        patient <- add_row( patient,
                            id = last_id,
                            patientid = patientid,
                            accountstatus =accountstatus,
                            cellphone=cellphone,
                            dateofbirth=dateofbirth,
                            clinic=clinic,
                            firstnames=firstnames,
                            homephone=homephone,
                            lastname=lastname,
                            race=race,
                            uuid=uuid         )
        
        ##popula a tabela patientidentifier
        
        id = last_id_pi +5
        patient_id = id 
        value  = novos_pacientes_por_inserir_farmac$patientid[i]
        type_id = 0
        
        patientidentifier <- add_row(patientidentifier, id=id, value = value, patient_id=patient_id,type_id = type_id)
        
        
        
        ##popula a tabela patientattribute
        
        id = last_id_pa + 5
        patient_att = last_id + 5
        value_att = novos_pacientes_por_inserir_farmac$datainiciotarv[i]
        type_id_attribute = 1
        
        patientattribute = add_row(patientiattribute, id = idpa, value =value_att, patient=patient_att, type_id =    type_id_attribute )
        
        
        # desnecessario
        # last_id  <<- id + 5
        # last_id_pi <<-id + 5
        # last_id_pa <<-id +5
        
        insertPatient(con_local, patient)
        isertPatientIdentifier (con_local, patientidentifier)
        isertPatientAttribute(con_local, patientattribute)
        
        
        
      }

        
    } else{
        
      message("Erro ao inserrir os pacientes na BD")
      save(logErro,file = 'logs/logErro.RData')
      }
      
      
      ## TODO
      ## Enviar email (farmaciamaputo@ccsaude.org.mz) com um xls em anexo do df LogReferencia
      
      # salvar o ficheiro dos logs dos pacientes referidos
      save(logReferencia,file = 'logs/logReferencia.RData')
    }else{
    
    # Do nothing
    # nao ha pacientes novos
    message('Nao ha pacientes novos referidos')
  }
  
  
  
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