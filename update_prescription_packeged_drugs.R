library(lubridate)

# Limpar o envinronment & inicializar as 

rm(list=setdiff(ls(), c("wd", "is.farmac") ))

source('config/config_properties.R')     


#####################################################################################################



if(!is.logical(con_local)){
  # busca todas dispensas nao evniadas para openmrs : imported =''
  
  dispenses_to_send_openmrs <- getDispensesToSendOpenMRS(con_local)
  
  
  if (nrow(dispenses_to_send_openmrs)> 0){
    
    # comecar a inserir da dispensa menos actualizada ate a ultima
    dispenses_to_send_openmrs <- dispenses_to_send_openmrs %>% arrange(patientid,desc(pickupdate)) 
    
    # pacientes do iDART para buscar o iD
    tmp_patients <- getPatientInfo(con_local)
    no_dups_dispenses <- distinct(dispenses_to_send_openmrs,as.Date(dispensedate), drugname, patientid , .keep_all = TRUE )
    no_dups_dispenses$'as.Date(dispensedate)' <- NULL
  
    all_patient_nids <- unique(no_dups_dispenses$patientid)

    # processa info de cada paciente
    for (i in 1:length(all_patient_nids)) {
      
      nid <- all_patient_nids[i]
      all_patient_dispenses <- no_dups_dispenses[which(no_dups_dispenses$patientid==nid),]
   
      
      all_patient_dispenses_dups <- all_patient_dispenses[duplicated(as.Date(all_patient_dispenses$dispensedate)),]
      drugs    <- dbGetQuery(con_local, 'select * from drug  ;')
      regimes  <- dbGetQuery(con_local, 'select * from regimeterapeutico where active=TRUE ;')
      
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
        prescription_id <- prescription_id + sample(7:15, 1)*3*(i+j)  ##  gerao aleatoria de ID apartir do ultimo
        
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
                 prescribed_drug_id <- prescribed_drug_id + sample(7:18, 1)*3*(i+j)  ##  gerao aleatoria de ID apartir do ultimo
                 prescribeddrugs_to_save <- composePrescribedDrugs( all_patient_dispenses[j,] , prescription_id )
                 
                 if( nrow(prescribeddrugs_to_save ) >0){
                   
                   status_pd <- saveNewPrescribedDrug(con_local,prescribeddrugs_to_save)
                   
                   if(status_pd){
                     message("Prescription and prescribed_drug saved sucessfully. ") 
                     # set current = 'F' on others prescription
                      dbSendQuery(con_local, paste0("update prescription set current ='F' where patient =",
                                                   prescription_to_save$patient[1], " and id <> ",prescription_id, " ; " ) )
                     
                     
                  
                     # processa package, packagedrugs & packaagedruginfotmp
                      # cria  packagedruginfotmp
                      packagedruginfotmp_to_save <- composePackageDrugInfoTmp(all_patient_dispenses[j,] ,user_admin$id[1])
        
                     # cria package
                     package_to_save <- composePackage(df.packagedruginfotmp = packagedruginfotmp_to_save[1,],
                                            prescription.to.save = prescription_to_save)
                     
                     # cria packageddrugs
                     packageddrugs_to_save <- composePackagedDrugs(df.packagedruginfotmp = packagedruginfotmp_to_save[1,],
                                           package.to.save = package_to_save)
                     
                     # actualiza campos packageid & packageddrug no df packagedruginfotmp
                     packagedruginfotmp_to_save$packageid[1]     <-  package_to_save$id[1]
                     packagedruginfotmp_to_save$notes[1]         <- all_patient_dispenses[j,]$notes[1]
                     packagedruginfotmp_to_save$packageddrug[1]  <- packageddrugs_to_save$id[1]
                       
                       
                       
                       status_p <-  savePackage(con_local, package_to_save )
                       if(status_p){
                         
                         status_pd <-  savePackagedDrugs(con_local, packageddrugs_to_save )
                         
                         if(status_pd){
                           status_pdit <- savePackageDrugInfoTmp(con_local,packagedruginfotmp_to_save )
                           if(status_pdit){
                             message('hurray!!!! everythings was saved')
                             
                             ## actualizar sync_temp dispense
                          
                                dispense_date <- as.Date(packagedruginfotmp_to_save[1,]$dispensedate[1])

                                 vec_id <- all_patient_dispenses[j,]$id[ 
                                  which( as.Date(all_patient_dispenses[j,]$dispensedate) ==dispense_date )]
                                 
                               if(length(vec_id)>1){
                                 
                                 for (t in 1:length(vec_id)) {
                                   
                                   id <- vec_id[t]
                                   sql_query <- paste0( "update sync_temp_dispense set imported = 'yes' where id = ",id, " ;" )
                                   print(sql_query)
                                   dbSendQuery(con_local,sql_query )
                                 }

                                 
                               } else {
                                 sql_query <- paste0( "update sync_temp_dispense set imported = 'yes' where id = ",vec_id[1], " ;" )
                                 print(sql_query)
                                 dbSendQuery(con_local,sql_query )
                                 
                               }
                       
                           
                           
                         } else {
                           ## rollback
                           dbSendQuery(con_local, paste0("delete from package where id =",package_to_save$id[1], " ;" ))
                           dbSendQuery(con_local, paste0("delete from prescribeddrugs where id =",prescribeddrugs_to_save$id[1], " ;" ))
                           dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                           message (paste0( 'Erro ao gravar package : Rollback status:') )
                           save(logErro, file = 'logs/logErro.RData')
                         }
                         
                       } else {
                         message ('Erro ao gravar package')
                         dbSendQuery(con_local, paste0("delete from package where id =",package_to_save$id[1], " ;" ))
                         dbSendQuery(con_local, paste0("delete from prescribeddrugs where id =",prescribeddrugs_to_save$id[1], " ;" ))
                         dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                         save(logErro, file = 'logs/logErro.RData')
                       }

                   } else { # roll back 
                     message ('Erro ao gravar package')
                     dbSendQuery(con_local, paste0("delete from prescribeddrugs where id =",prescribeddrugs_to_save$id[1], " ;" ))
                     dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                     save(logErro,file='logs/logErro.RData')
                     message("Erro ao salvar prescricao") 
                     
                   }
                 } else {
                   # roll back 
                   dbSendQuery(con_local, paste0(' delete from prescription where id =',prescription_id ,' ;' ))
                   save(logErro,file='logs/logErro.RData')
                   message("Erro ao salvar prescricao") 
                   
                   
                 } 
               } else {
                 save(logErro,file='logs/logErro.RData')
                 message("Erro ao salvar prescribeddrugs!") 
                 
               }

                 
               } else {
                 save(logErro,file='logs/logErro.RData')
                 message("Erro ao salvar prescricao!") 
                 
               }
               
               
             } 

      }
      
      } 
      
      ################################
      else { 
        

        # TODO ha prescricoes que tem mais de um medicamento
        load(file = 'config/linhast.RData')
        
        ## ******  Provider ID
        ############################################################################
        provider_id       <- getGenericProviderID(con_local)
        ## ******  Regime ID
        # elimina transicoes de DT se existirem
        patient_dispenses <- all_patient_dispenses
        index_dt_date <- which(patient_dispenses$weekssupply==12 )
        
        # elimina transicoes de DT se existirem
        if(length(index_dt_date)>0){
          
          dt_date <- patient_dispenses$dispensedate[index_dt_date[1]]
          dt_date <- as.Date(dt_date)
          next_dt_date <- dt_date %m+% months(3)
          
          df_transicoes_dt <- subset(patient_dispenses,  as.Date(dispensedate) > dt_date &  as.Date(dispensedate) < next_dt_date ,)
          ## actualizar sync_temp dispense
          for (t in 1:nrow(df_transicoes_dt)) {
            
            dispense_date <- as.Date(df_transicoes_dt$dispensedate[t])
            
            
            vec_id <- df_transicoes_dt$id[ 
              which( as.Date(df_transicoes_dt$dispensedate) ==dispense_date )]
            
            if(length(vec_id)>1){
              
              for (k in 1:length(vec_id)) {
                
                id <- vec_id[k]
                sql_query <- paste0( "update sync_temp_dispense set imported = 'yes' where id = ",id, " ;" )
                print(sql_query)
                dbSendQuery(con_local,sql_query )
              }
              
              
            } else {
              sql_query <- paste0( "update sync_temp_dispense set imported = 'yes' where id = ",vec_id[1], " ;" )
              print(sql_query)
              dbSendQuery(con_local,sql_query )
              
            }
            
            
          }
          
          patient_dispenses <- subset(patient_dispenses, ! as.Date(dispensedate) > dt_date &  as.Date(dispensedate) < next_dt_date ,)
          
          
        }
        
         vec_dates  <- sort( unique(as.Date(patient_dispenses$dispensedate)) )
        ################################################################################
        for (v in 1:length(vec_dates)) {
          
          # Processa as dispensas de cada data
          patient_dispense <- patient_dispenses[which(as.Date(patient_dispenses$dispensedate)==vec_dates[v]), ]
          
          patient <-  c(nid, patient_dispense$patientfirstname[1],patient_dispense$patientlastname[1])
          patient_id  <- getPatientId(tmp_patients,patient)
          
          if(patient_id==0){
            save(logErro,file='logs/logErro.RData')
            message ("paciente nao existe na BD local  gravar um erro . e passar para o proxmo") 
            break
          }
          
          ## ******  Regime ID
          ############################################################################
          regimet <- getRegimeID(df.regimes = regimes,regime.name =patient_dispense$regimeid[1] )
          if(class(regimet)=="data.frame"){
            
            regime_id <- regimet$regimeid[1]
            
          } else {
            regime_id <- getLastKnownRegimeID(con.local = con_local,patient.id =patient_id)
          }
          ## ******  LinhaT ID
          ############################################################################
          linha_id          <- getLinhaID(linhas,linha = patient_dispense$linhaid[1])
          ############################################################################
          ## ******  PrescriptionID 
          ############################################################################
          
          prescription_id <- getLastPrescriptionID(con_local)
          prescription_id <- prescription_id + sample(7:18, 1)*3*(i+v)  ##  gerao aleatoria de ID apartir do ultimo
          
          
            
            prescription_to_save <- composePrescription(df.dispense = patient_dispense[1,],
                                                        linha.id = linha_id,
                                                        regime.id = regime_id,
                                                        provider.id = provider_id,
                                                        patient.id = patient_id,
                                                        nid =nid,
                                                        prescription.id =prescription_id)
            # salva a prescricao
            if(nrow(prescription_to_save)>0){
              status <- saveNewPrescription(con_postgres = con_local,df.prescription =prescription_to_save )
              
              if(status){ # se salvou com sucesso, gravar a prescribed drugs 
                
                
                if(nrow(patient_dispense)>1){ # sao dois medicamentos por registar e kukuk
                  
                  
                  prescribed_drug_to_save <- composePrescribedDrugs(patient_dispense , prescription_id )
                  
              
                    
                    if( nrow(prescribed_drug_to_save ) >0){
                      
                      status_pd <- saveNewPrescribedDrug(con_local,prescribed_drug_to_save)
                      
                      if(status_pd){
                        message("Prescription and prescribed_drug saved sucessfully. ") 
                        # set current = 'F' on others prescription
                        dbSendQuery(con_local, paste0("update prescription set current ='F' where patient =",
                                                      prescription_to_save$patient[1], " and id <> ",prescription_id, " ; " ) )
                        
                        
                        
                        # processa package, packagedrugs & packaagedruginfotmp
                        # cria  packagedruginfotmp
                        packagedruginfotmp_to_save <- composePackageDrugInfoTmp(patient_dispense ,user_admin$id[1])
                        
                        # cria package
                        package_to_save <- composePackage(df.packagedruginfotmp = packagedruginfotmp_to_save,
                                       prescription.to.save = prescription_to_save)
                        
                        # cria packageddrugs
                        packageddrugs_to_save <-  composePackagedDrugs(df.packagedruginfotmp = packagedruginfotmp_to_save,
                                             package.to.save = package_to_save)
                        
                        # actualiza campos packageid & packageddrug no df packagedruginfotmp
                        
                        for (l in 1:nrow(prescribed_drug_to_save)) {
                          
                          packagedruginfotmp_to_save$packageid[l]     <-  package_to_save$id[1]
                          packagedruginfotmp_to_save$notes[l]         <-  patient_dispense[l,]$notes[1]
                          packagedruginfotmp_to_save$packageddrug[l]  <-  packageddrugs_to_save$id[l]
                          
                        }

                        status_p <-  savePackage(con_local, package_to_save )
                        
                        if(status_p){
                          
                          status_pd <-  savePackagedDrugs(con_local, packageddrugs_to_save )
                          
                          if(status_pd){
                            status_pdit <- savePackageDrugInfoTmp(con_local,packagedruginfotmp_to_save )
                            if(status_pdit){
                              message('hurray!!!! everythings was saved')
                              
                              ## actualizar sync_temp dispense
                              for (t in 1:nrow(packagedruginfotmp_to_save)) {
                                
                                dispense_date <- as.Date(packagedruginfotmp_to_save$dispensedate[t])
                               
                                
                                vec_id <- patient_dispense$id[ 
                                  which( as.Date(patient_dispense$dispensedate) ==dispense_date )]
                                
                                if(length(vec_id)>1){
                                  
                                  for (k in 1:length(vec_id)) {
                                    
                                    id <- vec_id[k]
                                    sql_query <- paste0( "update sync_temp_dispense set imported = 'yes' where id = ",id, " ;" )
                                    print(sql_query)
                                    dbSendQuery(con_local,sql_query )
                                  }
                                  
                                  
                                } else {
                                  sql_query <- paste0( "update sync_temp_dispense set imported = 'yes' where id = ",vec_id[1], " ;" )
                                  print(sql_query)
                                  dbSendQuery(con_local,sql_query )
                                  
                                }

                              
                            }
                            
                          } else {
                            ## rollback
                            dbSendQuery(con_local, paste0(' delete from prescribeddrugs where id = ',packeddrugs_to_save$id[1] ,' ;' ))
                            dbSendQuery(con_local, paste0("delete from package where id =",package_to_save$id[1], " ;" ))
                            dbSendQuery(con_local, paste0(' delete from prescribeddrugs where id = ',prescribed_drug_to_save$id[1] ,' ;' ))
                            dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                            message (paste0( 'Erro ao gravar package : Rollback status:') )
                            save(logErro, file = 'logs/logErro.RData')
                          }
                          
                        } else {
                          message ('Erro ao gravar packagedrugs')
                          dbSendQuery(con_local, paste0("delete from package where id =",package_to_save$id[1], " ;" ))
                          dbSendQuery(con_local, paste0("delete from prescribeddrugs where id = ",prescribeddrugs_to_save$id[1]," ;" ))
                          dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                          dbSendQuery(con_local, paste0(' delete from prescribeddrugs where id = ',prescribed_drug_to_save$id[1] ,' ;' ))
                          save(logErro, file = 'logs/logErro.RData')
                        }
                        
                      } else { # roll back 

                        dbSendQuery(con_local, paste0("delete from prescribeddrugs where id = ",prescribeddrugs_to_save$id[1]," ;" ))
                        dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                        save(logErro,file='logs/logErro.RData')
                        message("Erro ao salvar prescricao") 
                        
                      }
                    } else {
                      # roll back 
                      dbSendQuery(con_local, paste0(' delete from prescription where id =',prescription_id ,' ;' ))
                      save(logErro,file='logs/logErro.RData')
                      message("Erro ao salvar prescribeddrugs") 
                      
                      
                    }  
                     
                 
                  
                }   
                  else {
                      save(logErro,file='logs/logErro.RData')
                      message("Erro ao salvar prescribeddrugs") 
              
                      }

          
        
         
                } 
                else {  ####kukuk
                  
                  prescribed_drug_id <- getLastPrescribedDrugID(con_local)
                  prescribed_drug_id <- prescribed_drug_id + sample(7:18, 1)*3*(i+v)  ##  gerao aleatoria de ID apartir do ultimo
                  prescribeddrugs_to_save <- composePrescribedDrugs( patient_dispense[1,] , prescription_id )
                  
                  if( nrow(prescribeddrugs_to_save ) >0){
                    
                    status_pd <- saveNewPrescribedDrug(con_local,prescribeddrugs_to_save)
                    
                    if(status_pd){
                      message("Prescription and prescribed_drug saved sucessfully. ") 
                      # set current = 'F' on others prescription
                      dbSendQuery(con_local, paste0("update prescription set current ='F' where patient =",
                                                    prescription_to_save$patient[1], " and id <> ",prescription_id, " ; " ) )
                      
                      
                      
                      # processa package, packagedrugs & packaagedruginfotmp
                      # cria  packagedruginfotmp
                      packagedruginfotmp_to_save <- composePackageDrugInfoTmp(patient_dispense[1,] ,user_admin$id[1])
                      
                      # cria package
                      package_to_save <- composePackage(df.packagedruginfotmp = packagedruginfotmp_to_save[1,] ,
                                                        prescription.to.save = prescription_to_save)
                      
                      # cria packageddrugs kakakak
                      packeddrugs_to_save <- composePackagedDrugs(df.packagedruginfotmp = packagedruginfotmp_to_save[1,],
                                                                  package.to.save = package_to_save)
                      
                      # actualiza campos packageid & packageddrug no df packagedruginfotmp
                      packagedruginfotmp_to_save$packageid[1]     <-  package_to_save$id[1]
                      packagedruginfotmp_to_save$notes[1]         <- patient_dispense[1,]$notes[1]
                      packagedruginfotmp_to_save$packageddrug[1]  <- packeddrugs_to_save$id[1]
                      
                      
                      
                      status_p <-  savePackage(con_local, package_to_save )
                      if(status_p){
                        
                        status_pd <-  savePackagedDrugs(con_local, packeddrugs_to_save )
                        
                        if(status_pd){
                          status_pdit <- savePackageDrugInfoTmp(con_local,packagedruginfotmp_to_save )
                          if(status_pdit){
                            message('hurray!!!! everythings was saved')
                            
                            ## actualizar sync_temp dispense
                            
                            dispense_date <- as.Date(packagedruginfotmp_to_save$dispensedate[1])
                            
                            vec_id <- patient_dispense$id[ 
                              which( as.Date(patient_dispense$dispensedate) ==dispense_date )]
                            
                            if(length(vec_id)>1){
                              
                              for (t in 1:length(vec_id)) {
                                
                                id <- vec_id[t]
                                sql_query <- paste0( "update sync_temp_dispense set imported = 'yes' where id = ",id, " ;" )
                                print(sql_query)
                                dbSendQuery(con_local,sql_query )
                              }
                              
                              
                            } else {
                              sql_query <- paste0( "update sync_temp_dispense set imported = 'yes' where id = ",vec_id[1], " ;" )
                              print(sql_query)
                              dbSendQuery(con_local,sql_query )
                              
                            }
                            
                            
                          } else {
                            ## rollback
                            
                            dbSendQuery(con_local, paste0(' delete from prescribeddrugs where id = ',packeddrugs_to_save$id[1] ,' ;' ))
                            dbSendQuery(con_local, paste0("delete from package where id =",package_to_save$id[1], " ;" ))
                            dbSendQuery(con_local, paste0("delete from prescribeddrugs where id = ",prescribeddrugs_to_save$id[1]," ;" ))
                            dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                            message (paste0( 'Erro ao gravar package : Rollback status:') )
                            save(logErro, file = 'logs/logErro.RData')
                          }
                          
                        } else {
                          message ('Erro ao gravar package')
                          save(logErro, file = 'logs/logErro.RData')
                          dbSendQuery(con_local, paste0(' delete from package where id = ',package_to_save$id[1] ,' ;' ))
                          dbSendQuery(con_local, paste0(' delete from prescribeddrugs where id = ',prescribeddrugs_to_save$id[1] ,' ;' ))
                          dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                        }
                        
                      } else { # roll back 
                        dbSendQuery(con_local, paste0(' delete from prescribeddrugs where id = ',prescribeddrugs_to_save$id[1] ,' ;' ))
                        dbSendQuery(con_local, paste0(' delete from prescription where id = ',prescription_id ,' ;' ))
                        save(logErro,file='logs/logErro.RData')
                        message("Erro ao salvar prescricao") 
                        
                      }
                    } else {
                      # roll back 
                      dbSendQuery(con_local, paste0(' delete from prescription where id =',prescription_id ,' ;' ))
                      save(logErro,file='logs/logErro.RData')
                      message("Erro ao salvar prescribeddrugs") 
                      
                      
                    } 
                  } 
                  
                }
                

              } else {
                save(logErro,file='logs/logErro.RData')
                message("Erro ao salvar prescricao") 
                
              }
              
            }
            #################################################################################
 
    } 
    
        #################################################################################
      
    
    
    
    } 
      
      ################################
      
    
    
    } }   else {
    
    message('Sem dispensas por actualizar')
  }
 
  
} else {
 
   message('Erro de conexao local')
  save(logErro,file='logs/logErro.RData')
}