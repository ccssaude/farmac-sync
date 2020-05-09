
#' getFarmacServerCon -> Estabelece uma conexao com o servidor central - Farmac
#' 
#' @param postgres.user username do postgres
#' @param postgres.password passwd
#' @param postgres.db.name nome da db
#' @param postgres.host IP do servidor provincial (mail.ccsaude.org.mz)
#' @return con/FALSE  (con) - retorna um conexao valida  (FALSE) - erro de conexao   
#' @examples 
#' con_farmac <- getFarmacServerCon()
#' 
getFarmacServerCon <- function(){
  

  status <- tryCatch({
    
    
    # imprimme uma msg na consola
    message(paste0( "Postgres - conectando-se ao servidor FARMAC : ",
                    farmac.postgres.host, ' - db:',farmac.postgres.db.name, "...") )
    

    
    # Objecto de connexao com a bd openmrs postgreSQL
    con_postgres <-  dbConnect(PostgreSQL(),user = farmac.postgres.user,
                               password = farmac.postgres.password, 
                               dbname = farmac.postgres.db.name,
                               host = farmac.postgres.host,
                               port = farmac.postgres.port )
    
    return(con_postgres)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    
    message(paste0( "PosgreSQL - Nao foi possivel connectar-se ao host: ",
                    farmac.postgres.host, '  db:',farmac.postgres.db.name,
                    "...",'user:',farmac.postgres.user,
                    ' passwd: ', farmac.postgres.password)) 
    message(cond)
    
    # guardar o log 
    # se for um site de farmac entao no log guardamos o nome da FARMAC
    if(is.farmac){
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = ' getFarmacServerCon -> Estabelece uma conexao com o servidor central - Farmac',
                   error =as.character(cond$message) ) 
    } else {
      
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = ' getFarmacServerCon -> Estabelece uma conexao com o servidor central - Farmac',
                   error =as.character(cond$message) ) 
    }
 
  
    #Choose a return value in case of error
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
}



#' getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local
#' 
#' @param postgres.user username do postgres
#' @param postgres.password passwd
#' @param postgres.db.name nome da db no postgres
#' @param postgres.host localhost
#' @return FALSE/Con -  (con) retorna um conexao valida  (FALSE) - erro de conexao   
#' @examples 
#' con_local<- getLocalServerCon()
#' 
getLocalServerCon <- function(){
  

  status <- tryCatch({
    
    
    # imprimme uma msg na consola
    message(paste0( "Postgres - conectando-se ao servidor Local : ",
                    local.postgres.host, ' - db:',local.postgres.db.name, "...") )
    
    
    
    # Objecto de connexao com a bd openmrs postgreSQL
    con_postgres <-  dbConnect(PostgreSQL(),user = local.postgres.user,
                               password = local.postgres.password, 
                               dbname = local.postgres.db.name,
                               host = local.postgres.host,
                               port = local.postgres.port )
    
    return(con_postgres)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    
    # imprimir msg na consola
    
    message(paste0( "PosgreSQL - Nao foi possivel connectar-se ao host: ",
                    local.postgres.host, '  db:',local.postgres.db.name,
                    "...",'user:',local.postgres.user,
                    ' passwd: ', local.postgres.password)) 
    message(cond)
    
    
 
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  status
}



#' saveLogError -> guardar no log de erros  qualquer erro  que decorre durante a execucao
#' de um procedimento
#' 
#' @param us.name nome US
#' @param data.evento data em que o erro acontece
#' @param accao o que estava a tentar-se executar
#' @param erro msg de erro  
#' @return NA
#' @examples 
#' 
#' us_name <- 'CS Albazine'
#' data <- Sys.time()
#' action  <- 'get Farmac Patients
#' erro <- 'Can not connect to server + excption.msg '
#' saveLogError(us_name,data,action ,erro )
#' 
saveLogError <- function (us.name, event.date, action, error){
  
  # insere a linha de erro no log
  logErro  <<-  add_row(logErro,us = us.name, data_evento =event.date, accao =action, Erro= error)
  
}



#' sendLogError -> Envia log de erros para o servidor
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df.logdispense o datafrane com os errros de execucao (apenas os novas)  
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- sendLogError(con_postgres,logError)


sendLogError <- function(con_postgres , df.logerror ){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    dbWriteTable(con_postgres, "logerro", df.logerror , row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status <- TRUE
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    if(is.farmac){
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = ' sendLogError -> Envia log de erros para o servidor ',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = ' sendLogError -> Envia log de erros para o servidor',
                   error = as.character(cond$message) )  
    }
    
    
    #Choose a return value in case of error
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  return(status)
}


#' saveLogReferencia -> guardar informacao da referencia no log de pacientes referidos 
#' 
#' @param us.name nome US
#' @param data.evento data em que o erro acontece
#' @param patient paciente referido
#' @param us.ref us para onde foi referenciado referencia  
#' @return NA
#' @examples 
#' 
#' us_name <- 'CS Albazine'
#' data <- Sys.time()
#' paticoen  <- '12/456 - Marta Joao'
#' us_ref  <- 'Farmac Jardim'
#' saveLogReferencia(us_name,data,patient ,us_ref )
#' 
saveLogReferencia<- function (us.name, event.date, patient, us.ref){
  
  # insere a linha de erro no log
  logReferencia  <<-  add_row(logReferencia,unidade_sanitaria = us.name, data_evento =event.date, paciente =patient, referido_para= us.ref)
  
}


#' saveLogDispensa -> guardar informacao de dispensas  no log de dispensas  
#' 
#' @param clinic_name nome da us (farmac/us)
#' @param data.evento data em que o erro acontece
#' @param patient paciente 
#' @param dispense.date  data do levantamento na farmac 
#' @return NA
#' @examples 
#' 
#' data <- Sys.time()
#' patient  <- '12/456 - Marta Joao'
#' farmac.name  <- 'Farmac Jardim'
#' dispense.date data  do levantamento
#' saveLogDispensa(farmac.name,data,patient ,dispense.date )
#' 
saveLogDispensa<- function (clinic_name, event.date, patient, dispense.date){
  
  # insere a linha de erro no das dispensas
  log_dispensas  <<-  add_row(log_dispensas,unidade_sanitaria = clinic_name, data_evento = event.date, paciente =patient, data_levantamento= dispense.date)
  
}



#' sendLogDispense -> Envia log de dispensas para o servidor
#' 
#' @param con_postgres  obejcto de conexao com BD
#' @param df.logdispense o datafrane com as dispensas dos pacientes referidos (apenas as novas)  
#' @return TRUE/FALSE
#' @examples 
#' 
#' status <- sendLogDispense(con_farmac,logdispense)
#TODO addicionar bloco de warnings no trycatch

sendLogDispense <- function(con_postgres , df.logdispense ){
  
  # status (TRUE/FALSE)  envio com sucesso/ envio sem sucesso
  status <- tryCatch({
    
    status_send <- dbWriteTable(con_postgres, "logdispense", df.logdispense, row.names=FALSE, append=TRUE)
    
    ## se occorer algum erro , no envio esta parte nao vai executar
    status_send <- TRUE
    return(status_send)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    if(is.farmac){
      saveLogError(us.name = main_clinic_name,
                   event.date = as.character(Sys.time()),
                   action = 'sendLogDispense -> Envia log de dispensas do servidor farmac para o servidor Local ',
                   error = as.character(cond$message) )  
      
    } else {
      
      saveLogError(us.name = farmac_name,
                   event.date = as.character(Sys.time()),
                   action = 'sendLogDispense -> Envia log de dispensas da farmac para o servidor Farmac',
                   error = as.character(cond$message) )  
    }
    
    
    #Choose a return value in case of error
    return(FALSE)
  }, 
  warning = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # Se for um waring em que nao foi possivel buscar os dados guardar no log e return FALSE
    if(grepl(pattern = 'Could not create execute',x = cond$message,ignore.case = TRUE)){
      
      # guardar o log 
      if(fis.farmac){
        saveLogError(us.name = main_clinic_name,
                     event.date = as.character(Sys.time()),
                     action = 'sendLogDispense -> Envia log de dispensas do servidor FARMAC  para o servidor local ',
                     error = as.character(cond$message) )  
        
      } else {
        
        saveLogError(us.name = farmac_name,
                     event.date = as.character(Sys.time()),
                     action = 'sendLogDispense -> Envia log de dispensas da farmac para o servidor Farmac',
                     error = as.character(cond$message) )  
      }

      
      return(FALSE)
      
    } else {
      
      if (exists('status_send')){
        
        return(status_send)
      } else {
        return(FALSE)
      }
      
      
    }

  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  return(status)
}






#' getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.farmac  obejcto de conexao com BD iDART
#' @param clinic.name nome da US que referiu os pacientes 
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas de determinada US
#' @examples 
#' clinic_name <- 'CS BAGAMOIO'
#' con_farmac <- getFarmacServerCon()
#' farmac_log_dispense <- getLogDispenseFromServer(con_farmac,clinic_name)
#' 

getLogDispenseFromServer <- function(con.farmac, clinic.name) {
  
  
  log_dispenses <- tryCatch({
    
    
    temp_logs <- dbGetQuery( con.farmac , paste0("SELECT  unidade_sanitaria, data_evento, paciente, data_levantamento FROM public.logdispense where unidade_sanitaria='", clinic.name, "' ;" )  )
    
    return(temp_logs)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US',
                 error = as.character(cond$message) )  
    
    #Choose a return value in case of error
    return(FALSE)
  },
  warning = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US',
                 error = as.character(cond$message) )  
    
    
    # Se for um waring em que nao foi possivel buscar os dados guardar no log e return FALSE
    if(grepl(pattern = 'Could not create execute',x = cond$message,ignore.case = TRUE)){
      
      # guardar o log 
      saveLogError(us.name = clinic.name,
                   event.date = as.character(Sys.time()),
                   action = 'getLogDispenseFromServer -> Busca os logs  de dispensa envio/receber de uma det. US  ',
                   error = as.character(cond$message) )  
      
      return(FALSE)
      
    } else {
      
      if (exists('temp_logs')){
        
        return(temp_logs)
      }
      
      
    }
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  log_dispenses
  
  
}




#' getLogErrorFromServer -> Busca os logs de erro de uma det. US
#' no servidor FARMAC PosgreSQL 
#' 
#' @param con.farmac  obejcto de conexao com BD iDART
#' @param clinic.name nome da US que referiu os pacientes 
#' @return tabela/dataframe/df com todas dispensas da tabela sync_temp_dispensas de determinada US
#' @examples 
#' clinic_name <- 'CS BAGAMOIO'
#' con_farmac <- getFarmacServerCon()
#' farmac_log_dispense <- getLogErrorFromServer(con_farmac,clinic_name)
#' 

getLogErrorFromServer <- function(con.farmac, clinic.name) {
  
  
  log_errors <- tryCatch({
    
    
    temp_logs <- dbGetQuery( con.farmac , paste0("SELECT  us, data_evento, accao, erro FROM public.logerro where us = ''", clinic.name, "' ;" )  )
    
    return(temp_logs)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getLogErrorFromServer -> Busca os logs de erro de uma det. US ',
                 error = as.character(cond$message) )  
    
    #Choose a return value in case of error
    return(FALSE)
  },
  warning = function(cond) {
    
    ## Coisas a fazer se ocorrer um erro 
    
    # imprimir msg na consola
    message(cond$message)
    
    # guardar o log 
    saveLogError(us.name = clinic.name,
                 event.date = as.character(Sys.time()),
                 action = ' getLogErrorFromServer -> Busca os logs de erro de uma det. US',
                 error = as.character(cond$message) )  
    
    
    # Se for um waring em que nao foi possivel buscar os dados guardar no log e return FALSE
    if(grepl(pattern = 'Could not create execute',x = cond$message,ignore.case = TRUE)){
      
      # guardar o log 
      saveLogError(us.name = clinic.name,
                   event.date = as.character(Sys.time()),
                   action = 'getLogErrorFromServer -> Busca os logs de erro de uma det. US  ',
                   error = as.character(cond$message) )  
      
      return(FALSE)
      
    } else {
      
      if (exists('temp_logs')){
        
        return(temp_logs)
      }
      
      
    }
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  log_dispenses
  
  
}







#' getMainClinicName -> Busca o nome da US na tabela clinic
#'
#' 
#' @param con.local  obejcto de conexao com BD iDART
#' @return nome da us
#' @examples 
#' main_clinic_name<- getMainClinicName(con_local)
#' 

getMainClinicName <- function(con.local) {
  
  
  clinic_name   <- dbGetQuery( con.local ,"select clinicname from public.clinic where mainclinic = TRUE ; " )
    
    return(clinic_name$clinicname[1])
 
  
}

#' getGenericProvider -> Busca o id do provedor generico
#'
#' @param con.local  obejcto de conexao com BD iDART
#' @return id da provider generico
#' @examples 
#' generic_provider <- getGenericProvider(con_local)
#' 

getGenericProviderID <- function(con.local) {
  
  
  provider   <- dbGetQuery( con.local ,"select id as provider from public.doctor where active = TRUE ; " )
  
  return(as.numeric(provider$provider[1]))
  
  
}



#' getRegimeID -> Busca o id do regime 
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @param regimeesquema nome do esquema
#' @return id do regime
#' @examples 
#' regimeid <- getRegimeID(con_local, 'TDF+3TC+EFV')
#' 
# 
# getRegimeID <- function(con.local, regimeesquema) {
#   
#   
#   regime   <- dbGetQuery( con.local ,
#                           paste0("select regimeid from public.regimeterapeutico  where regimeesquema = '",regimeesquema,"' and active = TRUE ; " ))
#   
#   if(nrow(regime)>0){
#     return(as.numeric(regime$regimeid[1]))
#   } else {
#     return(0)
#   }
#   
#   
# }

#' getLinhas -> Busca todas linhas T
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @examples 
#' linha <- getLinhaID(con_local)
#' 

getLinhas <- function(con.local) {
  
  
  linha   <- dbGetQuery( con.local ,paste0("select * from linhat ; " ) )

  return(linha)

}

 
#' getPatientInfo -> Busca o id , nid , nomes e uuid de todos pacientes do iDART
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @examples 
#' patients <- getPatientInfo(con_local )
#' 

getPatientInfo <- function(con.local) {
  
  patients   <- dbGetQuery(
    con.local ,
    paste0(
      "select pat.id, patientid, firstnames, lastname, uuid ,startreason, pi.value  from public.patient  pat 
      inner join patientidentifier pi on pi.patient_id = pat.id 
      left join
        (
         select patient, max(startdate), startreason
           from episode
            group by patient, startreason
        )  ep on ep.patient = pat.id ; "
    )
  )
  
  # Remover transitos
  patients <-  patients[which(!patients$startreason %in%  c('Paciente em Transito', ' Inicio na maternidade')), ]
  dfTemp <-  patients[which(grepl(   pattern = "TR", ignore.case = TRUE,  x = patients$patientid  ) == TRUE), ]
  dfTemp_2 <- patients[which(grepl(  pattern = "VIS",ignore.case = TRUE,  x = patients$patientid ) == TRUE), ]
  dfTemp_3 <- patients[which(grepl( pattern = "VIA",  ignore.case = TRUE,  x = patients$patientid  ) == TRUE), ]

  
  patients <-  patients[which(!patients$patientid %in% dfTemp$patientid), ]
  patients <-    patients[which(!patients$patientid %in% dfTemp_2$patientid), ]
  patients <-  patients[which(!patients$patientid %in% dfTemp_3$patientid), ]

  
  rm(dfTemp_2, dfTemp, dfTemp_3)
  
  
  return(patients)
  
}



#' getLastKnownRegimeID -> Busca o id do regime da ultima precricao na BD
#'
#' @param con.local  obejecto de conexao com BD iDART
#' @param patient.id nid
#' @return id do regime
#' @examples 
#' regimeid <- getLastKnownRegimeID(con_local, '12/566')
#' 

getLastKnownRegimeID <- function(con.local,patient.id) {
  
  
  regime   <- dbGetQuery( con.local ,
                          paste0("SELECT regimeid  FROM public.prescription where patientid = ",
                          as.numeric(patient.id),
                          " order by date desc limit 1; " ))
  
  if(nrow(regime)>0){
    return(as.numeric(regime$regimeid[1]))
  } else {
    return(0)
  }
  
  
}

#' getRegimeID -> Busca o id do regime com base no nome do regime
#'
#' @param df.regimes df com regimes padronizados
#' @param regime.name name
#' @return id do regime
#' @examples 
#' regimeid <- getRegimeID(con_local, '12/566')
#' 

getRegimeID <- function(df.regimes ,regime.name) {
  
  regime  <- df.regimes[which(df.regimes$regimeesquema==regime.name),]
  if(nrow(regime)>0){
    if(nrow(regime)==1){
      return(regime)
    } else{
      
      tmp_regime= subset( df.regimes, regimeesquema==regime.name & active == TRUE ,)
      return(tmp_regime[1,])
    }
  } else{  # vamos procurar pela abreviatura ex : ABC+3TC+LPV/r(2DFC+LPV/r40/10) ->  ABC+3TC+LPV/r
    
    size <- nchar(regime.name)
    if(size>11){
      regime.name <- substr(regime.name, 1, 13 )
    }
    regime = subset(df.regimes, grepl(pattern = regime.name,x = df.regimes$regimeesquema,ignore.case = TRUE),)
    
    if(nrow(regime)==1){
      return(regime)
    } else if(nrow(regime)>1){
      dosage <- sub(".*(\\d+{2}).*$", "\\1", regime.name)
      
      regime = subset(df.regimes, grepl(pattern = regime.name,x = df.regimes$regimeesquema,ignore.case = TRUE) & grepl(pattern = dosage,x = df.regimes$regimeesquema,ignore.case = TRUE),)
      if(nrow(regime)>1) {
        return(regime[1,])
      } else {
        return(0)
      }

    } else { # pega o ulimo regime conhecido (ideia do colaco kkkk)
      return(0)
    }
  }
  
}

#' getLinhaID -> Busca o id da Linha 
#'
#' @param df.linhas  df com todos linhas T
#' @param linha  nome  da linha
#' @return id 
#' @examples 
#' id  <- getLinhaID(df.linhas,'1a Linha' ))
#' 

getLinhaID <- function(df.linhas,linha) {
  
  id <- df.linhas$linhaid[which(df.linhas$linhanome==linha)]
  id
}
  
#' getDrug -> Busca o drug pelo nome
#'
#' @param df.drugs  df com todos drugs T
#' @param linha  nome  da linha
#' @return id 
#' @examples 
#' id  <- getDrugId(df.drugs,'[LPV/RTV]' ))
#' 

getDrug<- function(df.drugs,drug.name) {
  
  drug <- df.drugs[which(df.drugs$name==drug.name),]
  if(nrow(drug)>0){
    if(nrow(drug)==1){
      return(drug)
    } else{
      
      tmp_drug = subset( df.drugs, name==drug.name & active == TRUE & pediatric =='F',)
      return(tmp_drug)
    }
  } else{  # vamos procurar pela abreviatura que esta dentro de parentesis rectos [TDF/3TC/DTG]
    
    first_index <- stri_locate_first(drug.name, regex = "\\[")[[1]]
    second_index <- stri_locate_first(drug.name, regex = "\\]")[[1]]
    dosage <- sub(".*(\\d+{2}).*$", "\\1", drug.name)
    
     abreviatura_drug <- substr(drug.name, first_index+1, second_index-1 )
     
     drug = subset(df.drugs, grepl(pattern = abreviatura_drug,x = df.drugs$name,ignore.case = TRUE),)
     if(nrow(drug)==1){
       return(drug)
     } else if(nrow(drug)>1){
       drug = subset(df.drugs, grepl(pattern = abreviatura_drug,x = df.drugs$name,ignore.case = TRUE) & df.drugs$pediatric=='F' ,)
       return(drug[1,])
     } else { # pega qualquer (ideia do colaco kkkk)
       return(df.drugs[1,])
     }
  }
}

#' getAllDrugs -> Busca todos medicamentos do iDART 
#'
#' @param con.postgres  objecto de conexao com  a bd
#' @return public.drugs 
#' @examples 
#' id  <- getAllDrugs(con_local)
#' 
# 
# getAllDrugs <- function(con.postgres) {
#   
#   
#   drugs   <- dbGetQuery( con.local , paste0("SELECT * from public.drug ; " ))
#   return(drugs)
#   
# }


#' getLastPrescriptionID -> Buscao id da ultima prescricao criada
#'
#' @param con.postgres  objecto de conexao com  a bd
#' @return id
#' @examples 
#' id  <- getLastPrescriptionID(con_local)
#' 

getLastPrescriptionID <- function(con.postgres) {
  
  
  id <- dbGetQuery( con.local , paste0("select id from prescription order by id desc limit 1; " ))
  return(id$id)
  
}





#' getPatientId -> Busca o id do paciente 
#'
#' @param df.temp.patients  df com todos pacientes do idart excluindo os transitos
#' @param patient c(nid,firstnames,lastname)
#' @return id 
#' @examples 
#' id  <- getPatientId(con_local, c('12/3444','Agnaldo Samuel', 'Macuacua'))
#' 

getPatientId <- function(df.temp.patients,patient) {
  
 id <- df.temp.patients$id[which(df.temp.patients$patientid==nid)][1]
 if(is.na(id)){
   id <- df.temp.patients$id[which(df.temp.patients$value==nid)][1]
   if(is.na(id)){
     pat <- subset(df.temp.patients,firstnames==patient[2]  & lastname ==patient[3])
     if(nrow(pat)==1){
       id <- pat$id[1]
       return(id)
       
     } else if(nrow(pat)>1){# paciente duplicado
       
       if(pat$uuid[1]==pat$uuid[2]){ # sao pacientes iguais, considere o primeiro
         
         id <- pat$id[1]
         
         if(is.farmac){
           
           saveLogError( us.name = farmac_name,    event.date =as.character(Sys.Date()),
                         action = 'getPatientId -> Busca o id do paciente',
                         error =  paste0('ERR_DUP Paciente FARMAC duplicado - ',pat$patientid[1])      )
         } else {
           saveLogError( us.name = main_clinic_name,    event.date = as.character(Sys.Date()),
                         action = 'getPatientId -> Busca o id do paciente',
                         error =  paste0('ERR_DUP Paciente FARMAC duplicado - ',pat$patientid[1])      )
         }
         
         return(id)
         
       } else {
         
         
         if(is.farmac){
           
           saveLogError( us.name = farmac_name,    event.date =as.character(Sys.Date()),
                         action = 'getPatientId -> Busca o id do paciente',
                         error =  paste0('ERR_DUP Paciente FARMAC duplicado - ',pat$patientid[1])      )
         } else {
           saveLogError( us.name = main_clinic_name,    event.date = as.character(Sys.Date()),
                         action = 'getPatientId -> Busca o id do paciente',
                         error =  paste0('ERR_DUP Paciente FARMAC duplicado - ',pat$patientid[1])      )
         }
         
         return(0)  # error
         
       }

         
     } else {
       
       return(0)  # no patient found
     }
     
   } else{
       return(id)
     }

 }else{
   return(id)
 }

}




#' composePrescription -> compoe um dataframe de prescricao para actualizar
#'
#' @param df.dispense  df dispensa do paciente
#' @param patient.id id do paciente
#' @param provider.id  id do doctor
#' @param linha.id id linha
#' @return NA 
#' @examples 
#' id  <- composePrescription(df.dispense,linha.id, regime.id,provider.id, patient.id)
#' 

composePrescription <- function(df.dispense,linha.id, regime.id,provider.id, patient.id, nid) {
 
  load('config/prescription.Rdata')


  precription <- add_row( precription,
                         clinicalstage      =0,
                         current            = 'T',
                         date               = df.dispense$date[1],
                         doctor             =provider.id,
                         duration           =  df.dispense$duration[1],
                         modified           =   df.dispense$modified[1],
                         patient            = patient.id,
                         prescriptionid     =    paste0(nid, '-',df.dispense$date[1]," - Farmac"  )  ,
                         reasonforupdate    = df.dispense$reasonforupdate[1],
                         notes              = paste0( 'FARMAC - ',gsub (pattern = 'NA',replacement = '',x = df.dispense$notes[1])), 
                         enddate            = df.dispense$enddate[1],    
                         drugtypes          = df.dispense$drugtypes[1],    
                         regimeid           = regime.id,
                         datainicionoutroservico = df.dispense$datainicionoutroservico[1],  
                         motivomudanca      = df.dispense$motivomudanca[1], 
                         linhaid            = linha.id,
                         ppe                = df.dispense$ppe[1],   
                         ptv                = df.dispense$ptv[1],  
                         tb                 = df.dispense$tb[1],  
                         tpi                = df.dispense$tpi[1],   
                         tpc                = df.dispense$tpc[1],
                          gaac              = df.dispense$gaac[1], 
                         af                 = df.dispense$af[1],    
                         ca                 = df.dispense$ca[1],         
                         ccr                = df.dispense$ccr[1], 
                         saaj               = df.dispense$saaj[1],   
                         fr                 =   df.dispense$fr[1] )
 
   
     return(precription)

  
}





composePrescribedDrugs <- function(df.dispense, prescription.id) {
  load('config/prescribeddrugs.RData')
  
  load(file = 'config/drugs.RData') 
  
  for(i in 1:nrow(df.dispense)){
    
    drug_name <- df.dispense$drugname[i]
    drug <- getDrug(df.drugs =drugs,drug.name =drug_name)
    amt =0
    if(drug$packsize[1]>30){
      amt =2
    } else {
      amt =1
    }
    prescribeddrugs <<- add_row(prescribeddrugs,
      amtpertime  =amt,
      drug = drug$id[1]  ,
      prescription = prescription.id,
      timesperday =df.dispense$timesperday[i],
      modified =  df.dispense$modified[i] ,
      prescribeddrugsindex = i
      )
    
      
  }
  
 #return( prescribeddrugs)
}