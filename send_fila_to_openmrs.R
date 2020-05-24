library(lubridate)
library(RMySQL)
# Limpar o envinronment & inicializar as 

rm(list=setdiff(ls(), c("wd", "is.farmac") ))

source('config/config_properties.R')     

source(file = 'Functions/generic_functions.R')
#####################################################################################################
# default file='config/jdbc.properties'
jdbc_properties = readJdbcProperties()
openmrs.user ='esaude'                           # ******** modificar
openmrs.password='esaude'                         # ******** modificar
openmrs.db.name='albazine'                          # ******** modificar
openmrs.host='172.18.0.2'                        # ******** modificar
openmrs.port=3306                                # ******** modificar

# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)

if(!is.logical(con_openmrs)){

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
    no_dups_dispenses$dateexpectedstring <-  as.Date(  no_dups_dispenses$dateexpectedstring, "%d %b %Y")
    all_patient_nids <- unique(no_dups_dispenses$patientid)
    drugs    <- dbGetQuery(con_local, 'select * from drug  ;')
    regimes  <- dbGetQuery(con_local, 'select * from regimeterapeutico where active=TRUE ;')
    
    # processa info de cada paciente
    ############################################
    for (i in 1:length(all_patient_nids)) {
      
      nid <- all_patient_nids[i]
      all_patient_dispenses <- no_dups_dispenses[which(no_dups_dispenses$patientid==nid),]
      #elimina casos de pacientes que levaram 2 frascos e tem 2 registos na sync_temp_dispense
      all_patient_dispenses <- distinct(all_patient_dispenses,as.Date(dispensedate), patientid , .keep_all = TRUE )

      patient <-  c(nid, all_patient_dispenses$patientfirstname[1],all_patient_dispenses$patientlastname[1])
      patient_id  <- getPatientId(tmp_patients,patient)
      if(patient_id==0){
        save(logErro,file='logs/logErro.RData')
        message ("paciente nao existe na BD local  gravar um erro . e passar para o proxmo") 
        break
      }
      patient_uuid <- tmp_patients$uuidopenmrs[which(tmp_patients$id==patient_id)][1]

      if(length(patient_uuid)>0){
        patient <- c(patient,patient_uuid)
      }
      else {
        
        saveLogError(us.name = main_clinic_name,
                     event.date = as.character(Sys.time()),
                     action = paste0('getPatientUuid - ', nid),
                     error = "Object with given uuid doesn't exist" )
        message ("paciente nao tem uuidopenmrs na tabela patient  gravar um erro . e passar para o proxmo") 
        break
      }
      
      # verifica se o uuid do paciente existe no openmrs
      if(checkPatientUuidExistsOpenMRS(jdbc.properties = jdbc_properties,patient = patient)){
        
        
        for (j in 1:nrow(all_patient_dispenses)) {
          
          ## ******  Regime UUId
          ############################################################################
          regimet <- getRegimeID(df.regimes = regimes,regime.name =all_patient_dispenses$regimeid[j] )
          if(class(regimet)=="data.frame"){
            
            regime_uuid <- regimet$regimenomeespecificado[1]
            
          } 
          else {
            temp_reg <- getLastKnownRegimeID(con.local = con_local,patient.id =patient_id)
            tmp_regimes <- dbGetQuery(con_local, paste0('select * from regimeterapeutico where regimeid=', as.numeric(temp_reg$id), ' ;'))
            tmp_regimes <- tmp_regimes$regimeesquema[1]
            regimet <- getRegimeID(df.regimes = regimes,regime.name =tmp_regimes )
            if(class(regimet)=="data.frame"){
              
              regime_uuid <- regimet$regimenomeespecificado[1]
              
            } else{
              regime_uuid = regimes[31,]$regimenomeespecificado # pega DTG
            }
            
          }
          
          # cria  packagedruginfotmp
          packagedruginfotmp_to_save <- composePackageDrugInfoTmpOpenMRS(all_patient_dispenses[j,] ,user_admin$id[1],regime_uuid)
          
          status <- sendFilaOpenMRS(jdbc.properties = jdbc_properties,df.patient = patient, df.fila.info = packagedruginfotmp_to_save)
          
          if(status){
            
            dbSendQuery(con_local, paste0("update sync_temp_dispense set openmrs_status = 'yes' 
                                          where dispensedate::date ='",as.Date(packagedruginfotmp_to_save$dispensedate),
                                          "' and patientid ='",
                                          nid, "' ;"
                                          ))
            
      
          }
          
          
          
        } 
        
        
      }


    } 
    
    
  }
  else { message('Sem dispensas por actualizar')}  
  

  
  
  }
else {
  
  message('Erro de conexao local')
  save(logErro,file='logs/logErro.RData')
  }
  
} else{
  message('Erro de conexao ao openmrs')
}