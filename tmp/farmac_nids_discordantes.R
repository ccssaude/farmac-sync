sync_temp_dispense <- dbGetQuery(con_local, paste0(" select distinct patientid, patientfirstname , patientlastname from
                                                  sync_temp_dispense sd  where sync_temp_dispenseid='", main_clinic_name ,"' ") )


idartAllPatients <- getAllPatientsIdart(con_local)

discordantes <- anti_join(sync_temp_dispense, idartAllPatients, by='patientid')



#match por nome e actualizar patientid na tabela sync_tem_dispense

if(nrow(discordantes)>0){
  discordantes$observacao  <- ""
  discordantes$novo_nid    <- ""
  discordantes$novo_nid_1  <- ""
  
  for (i in 1:nrow(discordantes)) {
    
    patient_id <- discordantes$patientid[i]
    f_name<- discordantes$patientfirstname[i]
    l_name <-  discordantes$patientlastname[i]
    #uuid <-  discordantes$uuid[i]
    # match por nome e apelid
    match <- idartAllPatients[which(tolower(idartAllPatients$firstnames)==tolower(f_name) & tolower(idartAllPatients$lastname)==tolower(l_name)),]
    
    if(nrow(match)==1){
      
      new_patient_id = match$patientid[1]
      
      #dbExecute(con_postgres, paste0("update sync_temp_dispense set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
      discordantes$observacao[i] <- paste0('actualizado para: ',new_patient_id)
      message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      discordantes$novo_nid[i] <- new_patient_id
    }
    else if(nrow(match)==2){
      
      if(match$uuid[1]==match$uuid[2]){
        new_patient_id = match$patientid[1]
       # dbExecute(con_postgres, paste0("update sync_temp_dispense set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
        discordantes$observacao[i] <-  paste0('actualizado para: ',new_patient_id)
        discordantes$novo_nid[i] <- new_patient_id
        message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      } else {
        new_patient_id = match$patientid[1]
        new_patient_id_2 = match$patientid[2]
        discordantes$novo_nid[i] <- new_patient_id
        discordantes$novo_nid_1[i] <- new_patient_id_2
       # discordantes$observacao[i] <- " o nid deste pacieente nao existe na tabela patient, e o match dos nomes e duplicado."
      }
      
      
    }
    else if(nrow(match)==0){
      
      discordantes$observacao[i] <- "este paciente nao existe na tabela patient. analizar e resolver manualmente"
      
    }
    else if(nrow(match)==3){
      
      if(match$patientid[1]==match$patientid[2] & match$patientid[2]==match$patientid[3] ){
        new_patient_id = match$patientid[1]
        #dbExecute(con_postgres, paste0("update sync_temp_dispense set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
        discordantes$observacao[i] <-  paste0('actualizado para: ',new_patient_id)
        discordantes$novo_nid[i] <- new_patient_id
        message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      } else {
        
        discordantes$observacao[i] <- " o nid deste pacieente nao existe na tabela patient, e o match dos nomes e triplicado"
      }
      
      
    }
    else {
      
      
      discordantes$observacao[i] <- "e o nid deste pacieente nao existe na tabela patient, e o match dos nomes e triplicado"
    }
    
    
  }
  #write_xlsx(x = discordantes,path = 'output/correcao_sync_temp_patients.xlsx')
  
}
