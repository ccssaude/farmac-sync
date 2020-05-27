##  passo 1 executar este script
source('config/config_properties.R')     


sync_temp_dispense <- dbGetQuery(con_local,"select * from sync_temp_dispense  where sync_temp_dispenseid='CS ALBASINE' ;")

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
  nid_antigo = no_nid_exist$patientid[k]
  count_no_match =0
  count_match =0 
  match = patients[which(tolower(patients$firstnames) ==tolower(patientfirstname) & tolower(patients$lastname) ==tolower(patientlastname) ),]
  if(nrow(match)==0){
    count_no_match= count_no_match+1
    print('no macth')
  } else if(nrow(match)==1){
    no_nid_exist$nid_tbl_patient_1[k] <- match$patientid[1]
    no_nid_exist$uuid_tbl_patient_1[k] <- match$uuid[1]
    print('Match !')
    dbExecute(con_local, paste0("update sync_temp_dispense set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
    dbExecute(con_local, paste0("update sync_temp_patients set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
    
  }else if(nrow(match)==2){
    no_nid_exist$nid_tbl_patient_1[k] <-match$patientid[1]
    no_nid_exist$uuid_tbl_patient_1[k] <- match$uuid[1]
    no_nid_exist$nid_tbl_patient_2[k] <-match$patientid[2]
    no_nid_exist$uuid_tbl_patient_2[k] <- match$uuid[2]
    
    index_last <- stri_locate_last(nid_antigo, regex = "/")[[1]]
    
    cod_seq <- substr(nid_antigo,index_last +2, nchar(nid_antigo) )
    no_nid_exist$nid_tbl_patient_3[k] =cod_seq
    
    if(grepl(pattern =cod_seq, x = match$patientid[1] )){
      
      dbExecute(con_local, paste0("update sync_temp_dispense set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
      dbExecute(con_local, paste0("update sync_temp_patients set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
      
    } else if(grepl(pattern =cod_seq, x = match$patientid[2] ) ){
      
      dbExecute(con_local, paste0("update sync_temp_dispense set patientid='", match$patientid[2] , "' where patientid = '", nid_antigo, "' ;"))
      dbExecute(con_local, paste0("update sync_temp_patients set patientid='", match$patientid[2] , "' where patientid = '", nid_antigo, "' ;"))
      
    } else{
      
      print(" nao encontrei este paciente apesar de se r duplicado")
    }
    
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

##  passo  executar este  codigos
#-- juraida 
dbExecute(con_local,"update sync_temp_dispense set patientid='0111041101/2019/00238' where  patientid='0111040701/2019/00238' ;")
dbExecute(con_local,"sync_temp_patients set patientid='0111041101/2019/00238' where  patientid='0111040701/2019/00238';")


#-- joao antonio
dbExecute(con_local,"update sync_temp_dispense set patientid='0111041101/2013/00183' where  patientid='11040701/13/0183';")
dbExecute(con_local,"update sync_temp_patients set patientid='0111041101/2013/00183' where  patientid='11040701/13/0183';")


# passo 3 

local.postgres.user ='postgres'                         # ******** modificar
local.postgres.password='postgres'                      # ******** modificar
local.postgres.db.name='farmac_magoanine'                          # ******** modificar
local.postgres.host='172.18.0.3'                        # ******** modificar
local.postgres.port=5432                                # ******** modificar

# Objecto de connexao com a bd o farmac magoani
con_farmac_mag <-  dbConnect(PostgreSQL(),user = local.postgres.user,
                             password = local.postgres.password, 
                             dbname = local.postgres.db.name,
                             host = local.postgres.host,
                             port = local.postgres.port )

farmac_mag_patients <- dbGetQuery(con_farmac_mag, " select pat.id, pat.patientid, pat.firstnames, pat.lastname, sy.mainclinicname from patient pat
                                                    left join sync_temp_patients sy on pat.patientid = sy.patientid")

farmac_mag_dispense <- dbGetQuery(con_farmac_mag, " select  syd.id, syd.patientid, syd.patientfirstname, syd.patientlastname, syd.sync_temp_dispenseid
                                                    from sync_temp_dispense syd  where syd.sync_temp_dispenseid='CS ALBASINE'    ")

farmac_mag_dispense <- distinct(farmac_mag_dispense,patientid , .keep_all = TRUE)


farmac_not_us <- anti_join(farmac_mag_dispense, patients, by='patientid')



# pacientes com nids na sync temp dispense que nao existe na tabela patient
#farmac_not_us <- anti_join(sync_temp_dispense,patients,by='patientid', ) %>% select('patientid','patientfirstname','patientlastname')
farmac_not_us$nid_tbl_patient_1 <-''
farmac_not_us$nid_tbl_patient_2 <-''
farmac_not_us$nid_tbl_patient_3 <-''
farmac_not_us$uuid_tbl_patient_1 <-''
farmac_not_us$uuid_tbl_patient_2 <-''
farmac_not_us$uuid_tbl_patient_3 <-''
#farmac_not_us <- farmac_not_us[ , -which( names(farmac_not_us) %in% c("nid_tbl_patient"))]

for(k in 1:nrow(farmac_not_us)){
  
  patientfirstname = farmac_not_us$patientfirstname[k] 
  patientlastname = farmac_not_us$patientlastname[k]
  nid_antigo = farmac_not_us$patientid[k]
  count_no_match =0
  count_match =0 
  match = patients[which(tolower(patients$firstnames) ==tolower(patientfirstname) & tolower(patients$lastname) ==tolower(patientlastname) ),]
  if(nrow(match)==0){
    count_no_match <<- count_no_match+1
    print('no macth')
  } else if(nrow(match)==1){
    farmac_not_us$nid_tbl_patient_1[k] <- match$patientid[1]
    farmac_not_us$uuid_tbl_patient_1[k] <- match$uuid[1]
    print('Match !')
    dbExecute(con_farmac_mag, paste0("update sync_temp_dispense set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
    dbExecute(con_farmac_mag, paste0("update patientidentifier set value='", match$patientid[1] , "' where value = '", nid_antigo, "' ;"))
    dbExecute(con_farmac_mag, paste0("update patient set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
    dbExecute(con_farmac_mag, paste0("update sync_temp_patients set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
    
    
    farmac_not_us$nid_tbl_patient_3[k] <- "actualizado"
  }else if(nrow(match)==2){
    farmac_not_us$nid_tbl_patient_1[k] <-match$patientid[1]
    farmac_not_us$uuid_tbl_patient_1[k] <- match$uuid[1]
    farmac_not_us$nid_tbl_patient_2[k] <-match$patientid[2]
    farmac_not_us$uuid_tbl_patient_2[k] <- match$uuid[2]
    
    index_last <- stri_locate_last(nid_antigo, regex = "/")[[1]]
    
    cod_seq <- substr(nid_antigo,index_last +2, nchar(nid_antigo) )
    #farmac_not_us$nid_tbl_patient_3[k] =cod_seq
    
    if(grepl(pattern =cod_seq, x = match$patientid[1] )){
      
      dbExecute(con_farmac_mag, paste0("update sync_temp_dispense set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
      dbExecute(con_farmac_mag, paste0("update patientidentifier set value='", match$patientid[1] , "' where value = '", nid_antigo, "' ;"))
      dbExecute(con_farmac_mag, paste0("update patient set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
      dbExecute(con_farmac_mag, paste0("update sync_temp_patients set patientid='", match$patientid[1] , "' where patientid = '", nid_antigo, "' ;"))
      
      farmac_not_us$nid_tbl_patient_3[k] <- "actualizado"
    } else if(grepl(pattern =cod_seq, x = match$patientid[2] ) ){
      
      dbExecute(con_farmac_mag, paste0("update sync_temp_dispense set patientid='", match$patientid[2] , "' where patientid = '", nid_antigo, "' ;"))
      dbExecute(con_farmac_mag, paste0("update patientidentifier set value='", match$patientid[2] , "' where value = '", nid_antigo, "' ;"))
      dbExecute(con_farmac_mag, paste0("update patient set patientid='", match$patientid[2] , "' where patientid = '", nid_antigo, "' ;"))
      dbExecute(con_farmac_mag, paste0("update sync_temp_patients set patientid='", match$patientid[2] , "' where patientid = '", nid_antigo, "' ;"))
      farmac_not_us$nid_tbl_patient_3[k] <- "actualizado"
    } else{
      farmac_not_us$nid_tbl_patient_3[k] =" nao actualizado"
      print(" nao encontrei este paciente apesar de ser duplicado")
      
    }
    
  } else if(nrow(match)==3){
    farmac_not_us$nid_tbl_patient_1[k] <-match$patientid[1]
    farmac_not_us$uuid_tbl_patient_1[k] <- match$uuid[1]
    farmac_not_us$nid_tbl_patient_2[k] <-match$patientid[2]
    farmac_not_us$uuid_tbl_patient_2[k] <- match$uuid[2]
    farmac_not_us$nid_tbl_patient_3[k] <-match$patientid[3]
    farmac_not_us$uuid_tbl_patient_3[k] <- match$uuid[3]
    
  }  else {
    farmac_not_us$nid_tbl_patient_3[k] =" nao actualizado"
  }
  
}








#  marta armando
dbExecute(con_farmac_mag," sync_temp_dispense set patientid='0111040701/2014/01850' where  patientid='011040701/2014/01850';")
dbExecute(con_farmac_mag," sync_temp_patients set patientid='0111040701/2014/01850' where  patientid='011040701/2014/01850';")


# elina mateus

dbExecute(con_farmac_mag,"update sync_temp_dispense set patientid='0111041101/2010/01614' where  patientid='011041101/2010/01614';")
dbExecute(con_farmac_mag,"update sync_temp_patients set patientid='0111041101/2010/01614' where  patientid='011041101/2010/01614';")
dbExecute(con_farmac_mag,"update patient set patientid='0111041101/2010/01614' where  patientid='011041101/2010/01614';")
dbExecute(con_farmac_mag,"update patientidentifier set value='0111041101/2010/01614' where  value='011041101/2010/01614';")


# especiosa 

dbExecute(con_farmac_mag,"update sync_temp_dispense set patientid='0111041101/2010/00073' where  patientid='01110401101/2010/00073';")
dbExecute(con_farmac_mag,"update sync_temp_patients set patientid='0111041101/2010/00073' where patientid='01110401101/2010/00073';")
dbExecute(con_farmac_mag,"update patient set patientid='0111041101/2010/00073' where  patientid='01110401101/2010/00073';")
dbExecute(con_farmac_mag,"update patientidentifier set value='0111041101/2010/00073'   where value='01110401101/2010/00073';")


# serafina mangue
dbExecute(con_farmac_mag,"update sync_temp_dispense set patientid='0111041101/2015/00414' where  patientid='0111040701/2015/00414';")
dbExecute(con_farmac_mag,"update sync_temp_patients set patientid='0111041101/2015/00414' where patientid='0111040701/2015/00414';")
dbExecute(con_farmac_mag,"update patient set patientid='0111041101/2015/00414' where patientid='0111040701/2015/00414';")
dbExecute(con_farmac_mag,"update patientidentifier set value='0111041101/2015/00414' where value='0111040701/2015/00414';")


# helena fabiao
dbExecute(con_farmac_mag,"update sync_temp_dispense set patientid='0111041101/2015/00163' where  patientid='11040701/15/0163';")
dbExecute(con_farmac_mag,"update sync_temp_patients set patientid='0111041101/2015/00163' where patientid='11040701/15/0163';")
dbExecute(con_farmac_mag,"update patient set patientid='0111041101/2015/00163' where patientid='11040701/15/0163';")
dbExecute(con_farmac_mag,"update patientidentifier set value='0111041101/2015/00163' where value='11040701/15/0163';")


# diziderio paulo
dbExecute(con_farmac_mag,"update sync_temp_dispense set patientid='0111041101/2012/00520' where  patientid='520/12';")
dbExecute(con_farmac_mag,"update sync_temp_patients set patientid='0111041101/2012/00520' where patientid='520/12';")
dbExecute(con_farmac_mag,"update patient set patientid='0111041101/2012/00520' where  patientid='520/12';")
dbExecute(con_farmac_mag,"update patientidentifier set value='0111041101/2012/00520' where value='520/12';")

# marcia armando
dbExecute(con_farmac_mag,"update sync_temp_dispense set patientid='0111040701/2014/01850' where  patientid='011040701/2014/01850';")
dbExecute(con_farmac_mag,"update sync_temp_patients set patientid='0111040701/2014/01850' where patientid='011040701/2014/01850';")
dbExecute(con_farmac_mag,"update patient set patientid='0111040701/2014/01850' where  patientid='011040701/2014/01850';")
dbExecute(con_farmac_mag,"update patientidentifier set value='0111040701/2014/01850' where value='011040701/2014/01850';")


# enia con_farmac_mag
dbExecute(con_farmac_mag,"update sync_temp_dispense set patientid='0111041101/2014/00572' where  patientid='0111040701/2014/00572';")
dbExecute(con_farmac_mag,"update sync_temp_patients set patientid='0111041101/2014/00572' where patientid='0111040701/2014/00572';")
dbExecute(con_farmac_mag,"update patient set patientid='0111041101/2014/00572' where  patientid='0111040701/2014/00572';")
dbExecute(con_farmac_mag,"update patientidentifier set value='0111041101/2014/00572' where value='0111040701/2014/00572';")






sync_temp_dispense = distinct(sync_temp_dispense, patientid, .keep_all = TRUE)

temp = anti_join(farmac_mag_dispense,sync_temp_dispense , by='patientid')


temp_2 = anti_join(temp,patients , by='patientid')
