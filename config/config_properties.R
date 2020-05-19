##  **************** Configuracao de Parametros para conexao com servidor PostgreSQL
##  **  farmac (mail.ccsaude.org.mz) 
##  **  local (localhost)
library(RPostgreSQL)

wd <- '~/Git/farmac-sync/'


# Set to TRUE/FALSE
is.farmac <- FALSE                                    # definir se o codigo vai executar na farmac ou nao


farmac.postgres.user ='farmac'                         # ******** modificar
farmac.postgres.password='iD@rt2020'                   # ******** modificar
farmac.postgres.db.name='pharm'                        # ******** modificar
farmac.postgres.host='mail.ccsaude.org.mz'             # ******** modificar
farmac.postgres.port=5455                              # ******** modificar


local.postgres.user ='postgres'                         # ******** modificar
local.postgres.password='postgres'                      # ******** modificar
local.postgres.db.name='albazine'                          # ******** modificar
local.postgres.host='172.18.0.3'                        # ******** modificar
local.postgres.port=5432                                # ******** modificar













































































################################################################################################################################################
#' Verifica se os ficheiros necessarios para executar as operacoes existem
#' 
#' @param files  nomes dos ficheiros
#'  @param dir  directorio onde ficam os files
#' @return TRUE/FALSE
#' @examples
#' default_loc = getOpenmrsDefaultLocation(con_openmrs)
checkScriptsExists <- function (files, dir){
  for(i in 1:length(files)){
    f <- files[i]
    if(!file.exists(paste0(dir,f))){
      message(paste0('Erro - Ficheiro ', f, ' nao existe em ',dir))
      return(FALSE)
    }
  }
  return(TRUE)
}
################################################################################################################################################

# set working directory - @ctiva o directorio wd


if (dir.exists(wd)){
  
  setwd(wd)  
  
if(checkScriptsExists(files = c('config/config_properties.R','get_dispenses.R','send_dispenses.R'),dir = wd)){
  
  
  
  source('Functions/dispense_functions.R')  ## Carregar funcoes
  source('Functions/generic_functions.R')             ## Carregar funcoes
  source('Functions/patient_functions.R')          ## Carregar funcoes
  
  # Load logs df
  load('logs/logErro.RData')
  load('logs/logDispensa.RData') 


    # Objecto de connexao com a bd openmrs postgreSQL
    con_local  <-  getLocalServerCon()

    
  if( ! is.logical(con_local)){
    
    user_admin <- getAdminUser(con_local)
    
    if(is.farmac){
      
      farmac_name = getMainClinicName(con.local = con_local)
    } else {
      
      main_clinic_name <- getMainClinicName(con.local = con_local)
    }
    
    
    con_farmac <- getFarmacServerCon()
    if(! is.logical(con_farmac)){
      
      message(paste0("Conexoes ", local.postgres.host," & ",farmac.postgres.host, " estabelecidas"))

    } 
    else {
      
      ## Houve problema de conexao...
      ## gravar os logs
      message("Algo correu mal, veja os erros na console")
      save(logErro,file = 'logs/logErro.RData')
      #rm(list=setdiff(ls(), c("wd", "is.farmac",) ))
} 
    
  }  else {
    
    ## Houve problema de conexao...
    ## gravar os logs
    message("Algo correu mal, veja os erros na console")
    save(logErro,file = 'logs/logErro.RData')
    rm(list=setdiff(ls(), c("wd", "is.farmac") ))
    
  } 
    
}    else{
  message( paste0('Ficheiros em falta. Veja o erro anterior'))
}

  
} else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
}
