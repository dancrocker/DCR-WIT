##############################################################################################################################
#     Title: ImportHoboAirTemps.R
#     Description: This script will process/import raw HOBO data files containing air temperature data
#     Output: df.wq - Daily air temp records imported to WQDB
#     Written by: Dan Crocker, July 2017, revised November 2017
##############################################################################################################################

# LOAD REQUIRED LIBRARIES
# library(RODBC)
# library(odbc)
# library(DBI)
# library(tidyverse)
# library(data.table)
# library(lubridate)
# library(DescTools)
# library(scales)
# library(readxl) # new to swap code that gets the data to read_excel()
#

# START PROCESSING THE DATA #
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable){

  path <- paste0(rawdatafolder, "/", file)
  # Read in the data to a dataframe
  df.wq <- read_excel(path, sheet = 1, col_names = TRUE, trim_ws = TRUE, range = cell_cols("A:D")) # This is the raw stage data

  # Remove first row which contains useless column headers, Take only Date and Airtemp columns
  df.wq <- df.wq[-1,c(2,4)]
  # Change column names
  names(df.wq) = c("DateTime", "AirTemp_C")
  df.wq <- df.wq[!df.wq$DateTime == "",]
  # Change data types to numeric
  df.wq[sapply(df.wq, is.character)] <- lapply(df.wq[sapply(df.wq, is.character)], as.numeric)
  # Convert DateTime format and update timezone
  df.wq$DateTime <- XLDateToPOSIXct(df.wq$DateTime)
  df.wq$DateTime <- force_tz(df.wq$DateTime, tzone = "America/New_York")
  
  # Filter out blank times due to daylight savings time 
  df.wq <- filter(df.wq, !is.na(DateTime))
  
  # Convert F to C
  df.wq$AirTemp_C <- round(5/9 * (df.wq$AirTemp_C -32), 2)

  # Get the tributary code for this air temp data
  trib <- substr(file,1,4)

  # Create new dataframe that calculates daily min, mean, max
  df.wq <- df.wq %>%
    group_by(date(df.wq$DateTime)) %>%
    summarize(AirTemp_C_min = min(AirTemp_C), AirTemp_C_mean = mean(AirTemp_C), AirTemp_C_max = max(AirTemp_C))

  # Change first column name to "Date"
  colnames(df.wq)[1] <- "DATE"
  # Assign the appropriate trib code
  df.wq$TRIBUTARY <- trib
  # Generate a new ID number
  con <-  odbcConnectAccess(filename.db)
  hobo <- sqlFetch(con, "tblHOBO_AIRTEMP")
  LastID <- as.numeric(max(hobo$ID))
  if(LastID == -Inf) {
    LastID <- 0
  } else {
    LastID <- LastID
  }
  df.wq$ID <- seq.int(nrow(df.wq)) + LastID
  #Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)
  # Make sure all column headers are uppercase and then rearrange columns and set data type to data frame
  names(df.wq) <- toupper(names(df.wq))
  df.wq <-  df.wq[, c(6,5,1, 2:4)]
  df.wq <- as.data.frame(df.wq)

   # Create a list of the processed datasets
  dfs <- list()
  dfs[[1]] <- df.wq
  dfs[[2]] <- path
  dfs[[3]] <- NA

  return(dfs)
} # PROCESS_DATA - STOP HERE AND INSPECT DATA BEFORE IMPORTING TO WQ DATABASE

#df.wq <- PROCESS_DATA(file, rawdatafolder, filename.db) # Run the function to process the air temp data

#
# INSPECT DATA TO MAKE SURE IT LOOKS GOOD BEFORE IMPORTING TO WQDB
#

##############
#IMPORT DATA #
##############
IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder,ImportTable, ImportFlagTable = NULL){

  # processedfolder <- "W:/WatershedJAH/EQStaff/WQDatabase/TribStages_HOBOs/Processed_HOBO_data"

  # Connect to db using RODBC
  con <-  odbcConnectAccess(filename.db)
  # Save the discharge and temp data to the table in the WQDatabase
  sqlSave(con, df.wq, tablename = "tblHOBO_AIRTEMP", append = TRUE,
          rownames = FALSE, colnames = FALSE, addPK = FALSE , fast = F)
  # Move the processed hobo file to the processed folder
  file.rename(path, paste0(processedfolder,"/",file))
  #Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)
}
