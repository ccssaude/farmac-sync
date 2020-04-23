require(RMySQL)
require(plyr)    
require(stringi)
require(RPostgreSQL)
require(stringr)
require(tidyr)
require(stringdist)
require(dplyr)  
require(writexl)

wd <- '~/farmac-sync/'      # ******** Configure para o dir onde deixou os ficheiros necessarios para executar o programa ****

## OpenMRS  - Configuracao de variaveis de conexao 
openmrs.user ='esaude'                          # ******** modificar
openmrs.password='esaude'                       # ******** modificar
openmrs.db.name='albazine'                      # ******** modificar
openmrs.host='127.17.0.2'                       # ******** modificar
openmrs.port=3333                               # ******** modificar

# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)


postgres.user ='postgres'                      # ******** modificar
postgres.password='postgres'                   # ******** modificar
postgres.db.name='albazine'                    # ******** modificar
postgres.host='127.17.0.3'                     # ******** modificar
postgres.port=5432                             # ******** modifica

con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)

































