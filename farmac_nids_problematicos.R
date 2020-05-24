
source('config/config_properties.R')     


sync_temp_dispense <- dbGetQuery(con_local,'select * from sync_temp_dispense ;')

patients <- dbGetQuery(con_local,'select * from patient ;')

# pacientes com nids na sync temp dispense que nao existe na tabela patient
no_nid_exist <- anti_join(sync_temp_dispense,patients,by='patientid', ) %>% select('patientid','patientfirstname','patientlastname')
no_nid_exist$nid_tbl_patient_1 <-''
no_nid_exist$nid_tbl_patient_2 <-''
no_nid_exist$nid_tbl_patient_3 <-''
no_nid_exist$uuid_tbl_patient_1 <-''
no_nid_exist$uuid_tbl_patient_2 <-''
no_nid_exist$uuid_tbl_patient_3 <-''
#no_nid_exist <- no_nid_exist[ , -which( names(no_nid_exist) %in% c("nid_tbl_patient"))]

for(k in 1:nrow(no_nid_exist)){
  
  patientfirstname = no_nid_exist$patientfirstname[k] 
  patientlastname = no_nid_exist$patientlastname[k]
  count_no_match =0
  count_match =0 
  match = patients[which(patients$firstnames ==patientfirstname & patients$lastname ==patientlastname ),]
  if(nrow(match)==0){
    count_no_match= count_no_match+1
    print('no macth')
  } else if(nrow(match)==1){
    no_nid_exist$nid_tbl_patient_1[k] <- match$patientid[1]
    no_nid_exist$uuid_tbl_patient_1[k] <- match$uuid[1]
    print('Match !')
  }else if(nrow(match)==2){
    no_nid_exist$nid_tbl_patient_1[k] <-match$patientid[1]
    no_nid_exist$uuid_tbl_patient_1[k] <- match$uuid[1]
    no_nid_exist$nid_tbl_patient_2[k] <-match$patientid[2]
    no_nid_exist$uuid_tbl_patient_2[k] <- match$uuid[2]

  } else if(nrow(match)==3){
    no_nid_exist$nid_tbl_patient_1[k] <-match$patientid[1]
    no_nid_exist$uuid_tbl_patient_1[k] <- match$uuid[1]
    no_nid_exist$nid_tbl_patient_2[k] <-match$patientid[2]
    no_nid_exist$uuid_tbl_patient_2[k] <- match$uuid[2]
    no_nid_exist$nid_tbl_patient_3[k] <-match$patientid[3]
    no_nid_exist$uuid_tbl_patient_3[k] <- match$uuid[3]
  }  else {
    print('4x')
  }
  
}
