
# busca todas dispensas nao evniadas para openmrs : imported =''

dispenses_to_send_openmrs <- getDispensesToSendOpenMRS(con_local)

if(!is.logical(dispenses_to_send_openmrs)){
  
  if (nrow(dispenses_to_send_openmrs)> 0){
    
    # pacientes do iDART para buscar o iD
    tmp_patients <- getPatientInfo(con_local)
    all_patient_nids <- unique(dispenses_to_send_openmrs$patientid)
    
    # processa info de cada paciente
    for (i in 1:length(all_patient_nids)) {
      
      nid <- all_patient_nids[i]

      all_patient_dispense <- dispenses_to_send_openmrs[which(dispenses_to_send_openmrs$patientid==nid),]
      
      
      
      # varias dispensas por pacientes ( pode ser varios medicamentos tambem)
      if(nrow(all_patient_dispense) > 1){
        
      } else {  
        
        patient <- c(nid, all_patient_dispense$patientfirstname[1],all_patient_dispense$patientlastname[1])
        patient_id        <- getPatientId(tmp_patients,patient)
        if(patient_id==0){
          save(logErro,file='logs/logErro.RData')
          stop("paciente nao existe na BD local  gravar um erro")  
          
        } else {
          load(file = 'config/prescription.Rdata')
          load(file = 'config/prescribeddrugs.RData')
          load(file = 'config/drugs.RData')
          load(file = 'config/regimes.RData')
          load(file = 'config/linhast.RData')
          ## ******  provider
          provider_id       <- getGenericProviderID(con_local)
          
          
          ## ******  regime
          regimet <- getRegimeID(df.regimes = regimes,regime.name =all_patient_dispense$regimeid[1] )
          
          if(class(regimet)=="data.frame"){
            
            regime_id <- regimet$regimeid[1]
            
          } else {
            regime_id <- getLastKnownRegimeID(con.local = con_local,patient.id =patient_id)
          }
          
          message(regime_id)
          
          ## ******  LinhaT
          
          linha_id          <- getLinhaID(linhas,linha =all_patient_dispense$linhaid[1])
          message(linha_id)
          
          prescription_to_save <- composePrescription(df.dispense = all_patient_dispense[1,],
                                              linha.id = linha_id,
                                              regime.id = regime_id,
                                              provider.id = provider_id,
                                              patient.id = patient_id,
                                              nid =nid
                                              )
          
          prescription_to_save$id <- NULL
          
           status <- saveNewPrescription(con_postgres = con_local,df.prescription =prescription_to_save )
           if(status){
             
             ## processa prescribed drugs
             
           }
          
        }

        
      }
      

      
      ## 99% dos casos retorna null
      prescription <- getPrescriptionFromPatient(con.local = con_local,date.pickup =pickup_date ,patient.id =patien.id )
      
      if(nrow(prescription)==0){

      
      }

      
      if(nrow(prescription)>0){
        message(paste0( 'found on prescriptioin in rec: ', i))
      }
      
      
    }
    
  } else {
    
    message('Sem dispensas por actualizar')
  }

  
  
  
} else {

  save(logErro,file='logs/logErro.RData')
}