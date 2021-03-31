###############################  HEADER  ######################################
#  TITLE: ImportManualFlags.R
#  DESCRIPTION: 
#  AUTHOR(S): Dan Crocker, Nick Zinck
#  DATE LAST UPDATED: 2020-12-31
#  GIT REPO: WIT
#  R version 3.5.3 (2019-03-11)  i386
##############################################################################.
# NOTE - THIS TOP SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT SECTION BELOW WHEN RUNNING FUNCTION IN SHINY

# Load libraries needed
# library(tidyverse)
# library(odbc)
# library(DBI)
# library(magrittr)

#############################
#   PROCESSING FUNCTION    #
############################

PROCESS_DATA <- function(flag.db, datatable, flagtable, flag, flagRecords, comment, usertype, userlocation){ # Start the function

   # probe is an optional argument
  options(scipen = 999) # Eliminate Scientific notation in numerical fields
  
### Connect to DB - need temporary logic to choose Access vs SQL Server  
  flag <- as.numeric(flag)
  
  dsn <- flag.db
  database <- "DCR_DWSP"
  schema <- userlocation
  tz <- 'UTC'
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)
  # Get current maximum record ID from the data table
  maxSampleID <- dbGetQuery(con, glue("SELECT max(ID) FROM [{schema}].[{datatable}]"))
  maxSampleID <- as.numeric(unlist(maxSampleID))
  # Get current maximum ID from the associated flag index table
  
  query.flags <- dbGetQuery(con, glue("SELECT max(ID) FROM [{schema}].[{flagtable}]"))
  
  dbDisconnect(con)
  rm(con)
  
  # Get current max ID
  if(is.na(query.flags)) {
    query.flags <- 0
  } else {
    query.flags <- query.flags
  }
  maxFlagID <- as.numeric(unlist(query.flags))
  rm(query.flags)

  # Make a dataframe to import to the flag index table - Send to UI for inspection
  df.manualflags <- data.frame(ID = seq.int(from = maxFlagID + 1, to = maxFlagID + length(flagRecords), by = 1),
                               DataTableName = datatable,  
                               SampleID = flagRecords,
                               FlagCode = flag,
                               DateFlagged = today(),
                               ImportStaff = as.character(username),
                               Comment = comment
                            )

 return(df.manualflags)

}
# df.manualflags <- PROCESS_DATA(flag.db, datatable, flagtable, flag, flagRecords, comment, usertype, userlocation)

IMPORT_DATA <- function(flag.db = flag.db, flagtable = flagtable, df.manualflags = df.manualflags, usertype, userlocation){
    start <- now()
    print(glue("Starting data import at {start}"))
    
    dsn <- flag.db
    database <- "DCR_DWSP"
    schema <- userlocation
    tz <- 'UTC'
    con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)
    odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{flagtable}")), value = df.manualflags, append = TRUE)
    dbDisconnect(con)
    rm(con)
    
    end <- now()
    return(print(glue("Import finished at {end}, \n elapsed time {round(end - start)} seconds")))  
}
# IMPORT_DATA(flag.db = flag.db, flagtable = flagtable, df.manualflags = df.manualflags)
