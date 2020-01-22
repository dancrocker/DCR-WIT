################################  HEADER  #####################################
#  TITLE: ImportStormFieldParameters
#  DESCRIPTION: Processes and imports raw YSI Field data into the WQ Database
#  AUTHOR(S): Dan Crocker
#  DATE LAST UPDATED:2020-01-14
#  GIT REPO: WIT
#  R version 3.5.3 (2019-03-11)  i386
##############################################################################.

# NOTE - THIS SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

### Load libraries needed ###
# library(tidyverse)
# library(stringr)
# library(odbc)
# library(RODBC)
# library(DBI)
# library(magrittr)
# library(lubridate)
# 
# ### SOURCE DATA/FUNCTIONS/FILES ####
# R_Config <- read.csv(Get from WAVE_WIT_Local, header = TRUE)
# config <- as.character(R_Config$CONFIG_VALUE)
# 
# # Function Args to provide:
# rawdatafolder <- Get from WAVE_WIT_Local
# files <- list.files(rawdatafolder, full.names = F) %>% print()
# file <- files[1]
# 
# ImportTable <- Get from WAVE_WIT_Local
# filename.db <- config[3]

#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#
  
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)

options(scipen = 999) # Eliminate Scientific notation in numerical fields

  # Get the full path to the file
path <- paste0(rawdatafolder,"/", file)
  
########################################################################.
###                     Read and Format Data                                     
########################################################################.

df.wq  <- read_csv(path, col_types = cols(
  Timestamp = col_character(),
  `Specific Conductance (uS/cm)` = col_double(),
  `Dissolved Oxygen (mg/L)` = col_double(),
  `pH_1 (Units)` = col_double(),
  `Temperature (C)` = col_double(),
  Comment = col_logical(),
  Site = col_character(),
  Folder = col_skip(),
  `Unit ID` = col_character(),
  `StormID` = col_character()
))

names(df.wq) <- c("DateTimeUTC", "Conductivity_uScm", "Dissolved_Oxygen_mgL",  "pH", "Water_temp_c", "Comment","Location","Unit_ID", "StormID")

### Format Date-Time stamp ####
*** NOTE *** BEWARE OF ANY DATA THAT SPANS DAYLIGHT SAVINGS TIME - AT THIS POINT
### WE DON'T KNOW HOW THE SENSOR HANDLES THIS TIME SHIFT. VERIFY IN MARCH AND MODIFY CODE. LINES BELOW MAY NEED
### TO GET MODIFIED TO ADD EITHER 4 OR 5 HRS TO EACH TIME AND NOT ADJUST TIME BY TIMEZONE

df.wq$DateTimeUTC <- parse_date_time(df.wq$DateTimeUTC,"%m/%d/%y %H:%M", tz = "America/New_York") # Use lima (UTC-5 to convert times - this avoids EST for America/New_York)
### Conver to UTC and round to nearest 15 minute increment
df.wq$DateTimeUTC <- as_datetime(df.wq$DateTimeUTC, tz = "UTC") %>% round_date(unit = "15 minutes")

########################################################################.
###                              Data Checks                        ####
########################################################################.     

  # At this point there could be a number of checks to make sure data is valid
  # Check to make all records have a StormID
  if (any(is.null(df.wq$StormID))) {
    # Send warning message to UI if TRUE
    stop("Storm IDs have not been provided for all of the records.\n You must add Storm IDs to the raw csv file before proceeding")
  }

########################################################################.
###                              Formatting                         ####
########################################################################.

### DataSourceID ###
# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(Location, DateTimeUTC)),]

# Connect to db for queries below
con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}",
                                            paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                 timezone = "America/New_York")

### IDs ####
  
setIDs <- function(){
    query.wq <- dbGetQuery(con, paste0("SELECT max(ID) FROM ", ImportTable))
    # Get current max ID
    if(is.na(query.wq)) {
      query.wq <- 0
    } else {
      query.wq <- query.wq
    }
    ID.max.wq <- as.numeric(unlist(query.wq))
    rm(query.wq)
    
    ### ID wq
    df.wq$ID <- seq.int(nrow(df.wq)) + ID.max.wq
  }
df.wq$ID <- setIDs()

### Reorder columns #### 
### 30 columns to match the database table exactly
col.order.wq <- dbListFields(con, ImportTable)
df.wq <-  df.wq[,col.order.wq]

### Always disconnect and rm connection when done with db
dbDisconnect(con)
rm(con)

### Create a list of the processed datasets ####
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- path
dfs[[3]] <- NULL # Removed condition to test for flags and put it in the setFlagIDS() function
dfs[[4]] <- data.frame(NULL,NULL) # Samples with site/time combo not matching any record in the database

return(dfs)

} # End Process Function 


#### COMMENT OUT SECTION BELOW WHEN RUNNING SHINY ###
# #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = ImportFlagTable)
# 
# # # Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]
# unmatchedtimes <- dfs[[4]]

########################################################################.
###                       Write data to Database                    ####
########################################################################.

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
  
  con <-  odbcConnectAccess(filename.db)
  
  ### WQ Data ####
  ColumnsOfTable <- sqlColumns(con, ImportTable)
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME)
  sqlSave(con, df.wq, tablename = ImportTable, append = T,
          rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)
  
  ### Flag data ####
  if (class(df.flags) == "data.frame"){ # Check and make sure there is flag data to import 
    sqlSave(con, df.flags, tablename = ImportFlagTable, append = T,
            rownames = F, colnames = F, addPK = F , fast = F, verbose = F)
  } else {
    print("There were no flags to import")
  }
  
  ### Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)
  
  # Move the raw data file to the processed folder ####
  processed_subdir <- paste0("/", max(year(df.wq$DateTimeUTC))) # Raw data archived by year, subfolders = Year
  file.create(processed_subdir)
  processed_dir <- paste0(processedfolder, processed_subdir)
  file.rename(path, paste0(processed_dir,"/", file))
  return("Import Successful")
}
### END ####

# processedfolder <- # Get from WAVE_WIT_Local 
 
# IMPORT_DATA(df.wq, df.flags, path, file, filename.db, processedfolder, ImportTable)
