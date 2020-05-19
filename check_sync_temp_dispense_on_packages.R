
##  actualiza todas sync_temp_dispense que ja foram inseridas na BD de modo a evitar duplicacoes de packages

all_packages <-  dbGetQuery(  con_local , 'select pat.patientid , p.prescription,  p.pickupdate from package p inner join prescription pr   on p.prescription=pr.id
inner join patient pat on pat.id =pr.patient order by pat.patientid ,  p.pickupdate ;'
  )

for (v in 1:nrow(dispenses_to_send_openmrs)) {
  
  nid = dispenses_to_send_openmrs$patientid[v]
  dispense_date <-  as.Date(dispenses_to_send_openmrs$dispensedate[v])
  packages <- all_packages[which(all_packages$patientid == nid), ]
  packages$pickupdate <- as.Date(packages$pickupdate)
  
  if (dispense_date %in%   packages$pickupdate) {
    dbSendQuery(
      con_local,
      paste0(
        "update sync_temp_dispense set imported = 'yes' where dispensedate::date = '",
        dispense_date,
        "' and patientid ='",nid, "' ;"
      )
    )
    
    
  }
  
  
}