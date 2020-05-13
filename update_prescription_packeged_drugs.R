
# busca todas dispensas nao evniadas para openmrs : imported =''

dispenses_to_send_openmrs <- getDispensesToSendOpenMRS(con_local)

if(!is.logical(dispenses_to_send_openmrs)){
  
  if (nrow(dispenses_to_send_openmrs)> 0){
    
    # pacientes do iDART para buscar o iD
    tmp_patients <- getPatientInfo(con_local)
    
    
    no_dups_dispenses <- distinct(dispenses_to_send_openmrs,as.Date(dispensedate), drugname, .keep_all = TRUE )
    no_dups_dispenses$'as.Date(dispensedate)' <- NULL
  
    all_patient_nids <- unique(no_dups_dispenses$patientid)
    

    # processa info de cada paciente
    for (i in 1:length(all_patient_nids)) {
      
      nid <- all_patient_nids[i]
      all_patient_dispenses <- no_dups_dispenses[which(no_dups_dispenses$patientid==nid),]
      all_patient_dispenses_dups <- all_patient_dispenses[duplicated(as.Date(all_patient_dispenses$dispensedate)),]
      drugs <- dbGetQuery(con_local, 'select * from drug  ;')
      regimes <- dbGetQuery(con_local, 'select * from regimeterapeutico where active=TRUE ;')
      
      # Todas linhas sao prescicao unica ( paciente levou um frasco)
      if(nrow(all_patient_dispenses_dups)==0){
        
        for (j in 1:nrow(all_patient_dispenses)) {
          
          
        patient <-  c(nid, all_patient_dispenses$patientfirstname[j],all_patient_dispenses$patientlastname[j])
        patient_id  <- getPatientId(tmp_patients,patient)
        
        if(patient_id==0){
          save(logErro,file='logs/logErro.RData')
          message ("paciente nao existe na BD local  gravar um erro . e passar para o proxmo") 
          break
        }
        ######################################################################
        ################### Load estrutura das tabelas
        # load(file = 'config/prescription.Rdata')
       # load(file = 'config/prescribeddrugs.RData')
        #load(file = 'config/regimes.RData')
        load(file = 'config/linhast.RData')
          
        ## ******  Provider ID
        ############################################################################
        provider_id       <- getGenericProviderID(con_local)
        ## ******  Regime ID
        ############################################################################
        regimet <- getRegimeID(df.regimes = regimes,regime.name =all_patient_dispenses$regimeid[j] )
        if(class(regimet)=="data.frame"){
          
          regime_id <- regimet$regimeid[1]
          
        } else {
          regime_id <- getLastKnownRegimeID(con.local = con_local,patient.id =patient_id)
        }
        ## ******  LinhaT ID
        ############################################################################
        linha_id          <- getLinhaID(linhas,linha = all_patient_dispenses$linhaid[j])
        ############################################################################\
        ## ******  PrescriptionID 
        ############################################################################
        prescription_id <- getLastPrescriptionID(con_local)
        prescription_id <- prescription_id + sample(1:15, 1)*(i+j)  ##  gerao aleatoria de ID apartir do ultimo
        
         prescription_to_save <- composePrescription(df.dispense = all_patient_dispenses[j,],
                                                linha.id = linha_id,
                                                regime.id = regime_id,
                                                provider.id = provider_id,
                                                patient.id = patient_id,
                                                nid =nid,
                                                prescription.id =prescription_id
                                                )
            
              # salva a prescricao
             if(nrow(prescription_to_save)>0){
               status <- saveNewPrescription(con_postgres = con_local,df.prescription =prescription_to_save )
               
               if(status){ # se salvou com sucesso, gravar a prescribed drugs e 
                 
                 prescribed_drug_id <- getLastPrescribedDrugID(con_local)
                 prescribed_drug_id <- prescribed_drug_id + sample(1:18, 1)*(i+j)  ##  gerao aleatoria de ID apartir do ultimo
                 composePrescribedDrugs( all_patient_dispenses[j,] , prescription_id, prescribed_drug_id )
                 prescribed_drug <- prescribeddrugs
                 
                 if( nrow(prescribed_drug ) >0){
                   
                   status_pd <- saveNewPrescribedDrug(con_local,prescribed_drug)
                   
                   if(status_pd){
                     message("Prescription and prescribed_drug saved sucessfully. ") 
                     # set current = 'F' on others prescription
                      dbSendQuery(con_local, paste0("update prescription set current ='F' where patient =",
                                                   prescription_to_save$patient[1], " and id <> ",prescription_id, " ; " ) )
                     
                     
                  
                     # processa package, packagedrugs & packaagedruginfotmp
                      # cria  packagedruginfotmp
                     composePackageDrugInfoTmp(all_patient_dispenses[j,] ,user_admin$id[1])
        
                     # cria package
                     composePackage(df.packagedruginfotmp = packagedruginfotmp,
                                            prescription.to.save = prescription_to_save)
                     
                     # cria packageddrugs
                     composePackagedDrugs(df.packagedruginfotmp = packagedruginfotmp,
                                           package.to.save = package,
                                           packageddrugsindex = 0)
                     
                     # actualiza campos packageid & packageddrug no df packagedruginfotmp
                      packagedruginfotmp$packageid[1]     <-  package$id[1]
                      packagedruginfotmp$notes[1]         <- all_patient_dispenses[j,]$notes[1]
                      packagedruginfotmp$packageddrug[1]  <- packageddrugs$id[1]
                       
                       
                       
                       status_p <-  savePackage(con_local, package )
                       if(status_p){
                         
                         status_pd <-  savePackagedDrugs(con_local, packageddrugs )
                         
                         if(status_pd){
                           status_pdit <- savePackageDrugInfoTmp(con_local,packagedruginfotmp )
                           if(status_pdit){
                             message('hurray!!!! everythings was saved')
                             return(TRUE)
                           }
                           
                         } else {
                           ## rollback
                           status_rollback <- dbSendQuery(con_local, paste0("delete from package where id =",package_to_save$id[1], " ;" ))
                           message (paste0( 'Erro ao gravar package : Rollback status:') )
                           save(logErro, file = 'logs/logErro.RData')
                         }
                         
                       } else {
                         message ('Erro ao gravar package')
                         save(logErro, file = 'logs/logErro.RData')
                       }

                   } else { # roll back 
                     dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                     save(logErro,file='logs/logErro.RData')
                     message("Erro ao salvar prescricao") 
                     
                   }
                 } else {
                   # roll back 
                   #dbSendQuery(con_local, paste0(' delete from prescription where id =',prescription_id ,' ;' ))
                   save(logErro,file='logs/logErro.RData')
                   message("Erro ao salvar prescricao") 
                   
                   
                 } 
               } else {
                 save(logErro,file='logs/logErro.RData')
                 message("Erro ao salvar prescricao") 
                 
               }

                 
               } else {
                 save(logErro,file='logs/logErro.RData')
                 message("Erro ao salvar prescricao") 
                 
               }
               
               
             } else {
               save(logErro,file='logs/logErro.RData')
               message("Erro ao salvar prescricao") 
               
             }
         
     
            
          }

        
      } else { # TODO ha prescricoes que tem mais de um medicamento
        
      }

      
      
      
    }
    
  } else {
    
    message('Sem dispensas por actualizar')
  }

  
  
  
} else {

  save(logErro,file='logs/logErro.RData')
}