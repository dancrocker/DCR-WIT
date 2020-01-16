##############################################################################################################################
#     Title: ImportManualFlags.R
#     Description: This script will create sample flag records for any dataset that has a related flag index table
#     Written by: Nick Zinck/Dan Crocker, October, 2017
#     Last Updated: May 2018
#
##############################################################################################################################

# NOTE - THIS TOP SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT SECTION BELOW WHEN RUNNING FUNCTION IN SHINY

# Load libraries needed
# library(tidyverse)
# library(odbc)
# library(RODBC)
# library(DBI)
# library(magrittr)

#############################
#   PROCESSING FUNCTION    #
############################

PROCESS_DATA <- function(flag.db, datatable, flagtable, flag, flagRecords){ # Start the function

   # probe is an optional argument
  options(scipen = 999) # Eliminate Scientific notation in numerical fields

   # Connect to db for queries below
  con <- dbConnect(odbc::odbc(),
                   .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}",
                  paste0("DBQ=", flag.db), "Uid=Admin;Pwd=;", sep = ";"),
                  timezone = "America/New_York")
  flag <- as.numeric(flag)
  # Get current maximum record ID from the data table
  maxSampleID <- dbGetQuery(con,paste0("SELECT max(ID) FROM ", datatable))
  maxSampleID <- as.numeric(unlist(maxSampleID))
  # Get current maximum ID from the associated flag index table

  query.flags <- dbGetQuery(con,paste0("SELECT max(ID) FROM ", flagtable))
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
                               ImportStaff = as.character(username)
                            )
# df.manualflags$ImportStaff <- as.character(df.manualflags$ImportStaff)
 # Disconnect from db and remove connection obj
 dbDisconnect(con)
 rm(con)

 return(df.manualflags)

}
# df.manualflags <- PROCESS_DATA(flag.db, datatable, flagtable, flag, flagRecords)

IMPORT_DATA <- function(flag.db = flag.db, flagtable = flagtable, df.manualflags = df.manualflags){

# Connect to db using ODBC
  con <-  odbcConnectAccess(flag.db)

# Save the new records
  ColumnsOfTable <- sqlColumns(con, flagtable)
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME)
    sqlSave(con, df.manualflags, tablename = flagtable, append = T,
            rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)

  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)

}
# IMPORT_DATA(flag.db = flag.db, flagtable = flagtable, df.manualflags = df.manualflags)















