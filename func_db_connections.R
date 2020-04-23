
library(RPostgreSQL)

#' Estabelece uma conexao com o servidor central
#' 
#' @param postgres.user username do postgres
#' @param postgres.password passwd
#' @param postgres.db.name nome da db no postgres
#' @param postgres.host IP do servidor provincial (mail.ccsaude.org.mz)
#' @return con/0  (con) - retorna um conexao valida  (0) - erro de conexao   
#' @examples 
#' con_server <-getFarmacServerCon ('postgres',  'passwd', 'pharm','mail.ccsaude.org.mz','5432')
#' 
getFarmacServerCon <- function(postgres.user,postgres.password,postgres.db.name,postgres.host,postgres.port){
  
  con_postgres <-  dbConnect(PostgreSQL(),
                             user = postgres.user,
                             password = postgres.password,
                             dbname = postgres.db.name,
                             host = postgres.host)
  
  return(con_postgres)
  
  
}






























