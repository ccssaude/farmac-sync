
########################################################### 

# Limpar o envinronment & inicializar as 

rm(list=setdiff(ls(), c("wd", "is.farmac") ))

source('config/config_properties.R')     

# buscar todas dispensas para actualziar aquelas que ja foram inseridas no openmrs

dispenses_to_send_openmrs <-     # get local dispenses
  sync_dispense_local <- getLocalSyncTempDispense(con_local )


########################################################### 

##  actualiza todas sync_temp_dispense que ja foram inseridas na BD de modo a evitar duplicacoes de packages

all_packages <-  dbGetQuery(  con_local , 'select dispensedate, patientid from packagedruginfotmp ;'
  )

dispenses_to_fix  <-  dispenses_to_send_openmrs %>% arrange(patientid,desc(pickupdate)) 
# dispenses_to_fix <- distinct(dispenses_to_fix,as.Date(dispensedate), drugname, patientid , .keep_all = TRUE )
# dispenses_to_fix$'as.Date(dispensedate)' <- NULL
dispenses_to_fix$dispensedate <- as.Date(dispenses_to_fix$dispensedate)

all_patient_nids <- unique(dispenses_to_fix$patientid)

for (v in 1:length(all_patient_nids)) {
  
  nid = all_patient_nids[v]
  packages <- all_packages[which(all_packages$patientid == nid), ]
  packages$dispensedate <- as.Date(packages$dispensedate)

  dates_to_fix <- sort (dispenses_to_fix$dispensedate[which(dispenses_to_fix$patientid==nid)], decreasing = TRUE)

  for (i in 1:length(dates_to_fix)) {
    
    dispense_date <- dates_to_fix[i]
    
    if (dispense_date %in%   packages$dispensedate) {
      
      dbSendQuery(
        con_local,
        paste0( "update sync_temp_dispense set imported = 'yes',openmrs_status = 'yes' where dispensedate::date = '",
                dispense_date,"' and patientid = '",nid, "' ;"  )    )
      
      
      print(  paste0( "update sync_temp_dispense set imported = 'yes' where dispensedate::date = '",
                      dispense_date,"' and patientid = '",nid, "' ;"  ) )
      # dbSendQuery(con_local, paste0("delete from packagedruginfotmp  where dispensedate::date = '",
      #                               dispense_date,"' and patientid = '",nid, "' ;"  ) )
      
      # package_id <-       packages$id[which(packages$pickupdate == dispense_date) ]
      # 
      # if(length(package_id)>1){
      #   
      #   for (v in 1:length(package_id)) {
      #     id <- package_id[v]
      # 
      #     packageddrug_id = dbGetQuery(con_local, paste0("select id from packageddrugs where parentpackage = ",as.numeric(id), " ;" ))
      #     if (nrow(packageddrug_id)>1){
      #       
      #       for (var in 1:nrow(packageddrug_id)) {
      #         id_pd <- packageddrug_id$id[var]
      #         dbSendQuery(con_local, paste0("delete from packagedruginfotmp  where packageddrug = ",as.numeric(id_pd), " ;"))
      #         dbSendQuery(con_local, paste0("delete from packageddrugs  where id = ",as.numeric(id_pd), " ;"))
      #       }                        
      #       
      # 
      #     } else{
      #         
      #       id_pd <- packageddrug_id$id[1]
      #       dbSendQuery(con_local, paste0("delete from packagedruginfotmp  where packageddrug = ",as.numeric(id_pd), " ;"))
      #       dbSendQuery(con_local, paste0("delete from packageddrugs  where id = ",as.numeric(id_pd), " ;"))
      #       
      #       }
      # 
      #     prescription_id  <-  packages$prescription[which(packages$id == as.numeric(id)) ]
      #     dbSendQuery(con_local, paste0("delete from package  where id = ",as.numeric(id), " ;"))
      #     
      #     if(length(prescription_id)>1){
      #       for (l in 1:length(prescription_id)) {
      #         id <- prescription_id[l]
      #         dbSendQuery(con_local, paste0("delete from prescribeddrugs  where prescription = ",as.numeric(id), " ;"))
      #         dbSendQuery(con_local, paste0("delete from prescription  where id = ",as.numeric(id), " ;"))
      #       }
      #     }
      #     else {
      #       dbSendQuery(con_local, paste0("delete from prescribeddrugs  where prescription = ",as.numeric(prescription_id[1]), " ;"))
      #       dbSendQuery(con_local, paste0("delete from prescription  where id = ",as.numeric(prescription_id[1]), " ;"))
      #     }
      #     
      #   }
      #   
      # } else {
      #   
      #   id = package_id[1]
      #   
      #   packageddrug_id = dbGetQuery(con_local, paste0("select id from packageddrugs where parentpackage = ",as.numeric(id), " ;" ))
      #   if (nrow(packageddrug_id)>1){
      #     
      #     for (var in 1:nrow(packageddrug_id)) {
      #       id_pd <- packageddrug_id$id[var]
      #       dbSendQuery(con_local, paste0("delete from packagedruginfotmp  where packageddrug = ",as.numeric(id_pd), " ;"))
      #       dbSendQuery(con_local, paste0("delete from packageddrugs  where id = ",as.numeric(id_pd), " ;"))
      #     }                        
      #     
      #     
      #   } else{
      #     
      #     id_pd <- packageddrug_id$id[1]
      #     dbSendQuery(con_local, paste0("delete from packagedruginfotmp  where packageddrug = ",as.numeric(id_pd), " ;"))
      #     dbSendQuery(con_local, paste0("delete from packageddrugs  where id = ",as.numeric(id_pd), " ;"))
      #     
      #   }
      #   
      #   
      #   prescription_id  <-  packages$prescription[which(packages$id == package_id[1]) ]
      #   dbSendQuery(con_local, paste0("delete from package  where id = ",package_id[1], " ;"))
      #   
      #   
      #   if(length(prescription_id)>1){
      #     for (k in 1:length(prescription_id)) {
      #       id <- prescription_id[k]
      #       dbSendQuery(con_local, paste0("delete from prescribeddrugs  where prescription = ",as.numeric(id), " ;"))
      #       dbSendQuery(con_local, paste0("delete from prescription  where id = ",as.numeric(id), " ;"))
      #     }
      #   }
      #   else {
      #     dbSendQuery(con_local, paste0("delete from prescribeddrugs  where prescription = ",as.numeric(prescription_id[1]), " ;"))
      #     dbSendQuery(con_local, paste0("delete from prescription  where id = ",as.numeric(prescription_id[1]), " ;"))
      #   }
      #   
      # }
      
      
     #######################################}
    
    
  }

  
  }
 }