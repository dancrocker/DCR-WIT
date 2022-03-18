###############################  HEADER  ######################################
#  TITLE: ImportFieldParameters.R
#  DESCRIPTION: This script will Format/Process/Import YSI, turbidity, and stage data
#        to the Database
#  AUTHOR(S): Dan Crocker
#  DATE LAST UPDATED: 2021-01-07
#  GIT REPO: WIT
#  R version 3.5.3 (2019-03-11)  i386
##############################################################################.

# NOTE - THIS SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

# Load libraries needed

# library(tidyverse)
# library(stringr)
# library(odbc)
# library(DBI)
# library(magrittr)
# library(openxlsx)
# library(DescTools)
# library(lubridate)
# library(devtools)
# library(glue)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

########################################################################.
###                          PROCESS DATA                  ####
########################################################################.
# NOTE: file is just filename, not full path
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)

options(scipen = 999) # Eliminate Scientific notation in numerical fields
# Path to raw data file
filepath <- paste0(rawdatafolder,"/", file)

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

pro_plus_header <- c("Timestamp", 
                     "Specific.Conductance..uS.cm.", 
                     "Dissolved.Oxygen..mg.L.",
                     "pH_1..Units.",                 
                     "Temperature..C.",
                     "Comment",
                     "Site",                         
                     "Folder",                       
                     "Unit.ID",
                     "SampledBy")

pro_quatro_header <- c("Date",
                       "Time",
                       "DataID",	
                       "Temp.C.",	
                       "DO..L.",	
                       "DO.mg.L.",	
                       "SPC.uS.cm.",
                       "pH",	
                       "Probe_ID",	
                       "SampledBy")


### Read File ####

data <- read.csv(filepath)

### Determine which filetype is in the csv ####
if(all(names(data) %in% pro_plus_header)) {
  print("Data is from a YSI Pro Plus device...")
  sensor <- "YSI Pro Plus" 
}  else {
  if(all(names(data) %in% pro_quatro_header)) {
    print("Data is from a YSI Pro Quatro device...")
    sensor <- "YSI Pro Quatro"
  } else {
    stop("Data header format unexpected. Please check that the necessary 'Probe_ID' and 'SampleBy' columns exist in the selected file")
  }
}

if(sensor =="YSI Pro Plus") {
  ### Format Pro Plus Data ####
  names(data)[2:5] <- c("Specific Conductance", "Dissolved Oxygen", "pH", "Water Temperature")
  
  df <- data %>% 
    select(-c(Folder, Comment)) %>%
    pivot_longer(cols = c(2:5), names_to = "Parameter", values_to = "FinalResult") %>%
    filter(!is.na(FinalResult)) %>% 
    dplyr::rename("Location" = Site, "DateTimeET" = Timestamp, "Probe_ID" = Unit.ID) %>% 
    mutate("Units" = NA_character_, 
           "Probe_ID" = paste0(sensor," - " , substrRight(Probe_ID, 1)),
           "DateTimeET" = mdy_hm(DateTimeET, tz = "America/New_York")
    )
} else {
  ### Format Pro Quatro Data #### 
  names(data)[4:8] <- c("Water Temperature", "Oxygen Saturation", "Dissolved Oxygen", "Specific Conductance", "pH")
  
  df <- data %>% 
    pivot_longer(cols = c(4:8), names_to = "Parameter", values_to = "FinalResult", values_drop_na = TRUE) %>%
    filter(!is.na(FinalResult)) %>% 
    dplyr::rename("Location" = DataID) %>%
    mutate("Units" = NA_character_, 
           "Location" = substrRight(Location, 4),
           "Probe_ID" = paste0(sensor," - " , Probe_ID),
           "DateTimeET" = mdy_hms(paste(Date, Time, sep = " "), tz = "America/New_York")
    ) %>% 
    select(-c("Date", "Time"))
}

### Connect to Database ####  
dsn <- "DCR_DWSP_App_R"
database <- "DCR_DWSP"
schema <- 'Wachusett'
tz <- 'UTC'
con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)

### Load tables from SQL Server ####
params <- dbReadTable(con, Id(schema = schema, table = "tblParameters"))
locations <- dbReadTable(con,  Id(schema = schema, table = "tblWatershedLocations"))
flowlocations <- filter(locations, !is.na(LocationFlow))
ratings <- dbReadTable(con, Id(schema = schema, table = "tblRatings"))

### Populate Units Column ####
df$Units <- params$ParameterUnits[match(df$Parameter, params$ParameterName)]

### Round Times ####
df$DateTimeET <- round_date(df$DateTimeET, "minute") 

df$DateTimeET <- ifelse(df$Location == "MD04",
                        round_date(df$DateTimeET, "10 minutes"),
                        ifelse(df$Location %in% flowlocations$LocationMWRA,
                               round_date(df$DateTimeET, "15 minutes"),
                               df$DateTimeET))

df$DateTimeET <- as_datetime(df$DateTimeET) %>% with_tz("America/New_York")

### Fix location names ####
df$Location %<>%
  gsub("M754","MD75.4",.)

if (any(!unique(df$Location) %in% locations$LocationMWRA)) {
  stop("There is at least one location in this data that is not yet recorded in the database. Either remove the record or rename the location to MISC and add result to MISC sample table after import.")
}

### Add missing columns ####
# Get columns from table in db
cols <- dbListFields(con, schema_name = schema, name = "tblTribFieldParameters")

# Add the ones that are missing
for (col in cols) {
  if(!col %in% names(df)) {
    df <- mutate(df, "{col}" := NA)
  }
}
### Order columns to match db ####

df <- df[,cols]

####################################.
#  START REFORMATTING THE DATA  ####
####################################.

# Fix data types
df$ID <- as.numeric(df$ID)
df$UniqueID <- as.character(df$UniqueID)
df$DataSource <- as.character(df$DataSource)
df$DataSourceID <- as.numeric(df$DataSourceID)
df$ImportDate <- as_date(df$ImportDate)
df$Imported_By <- as.character(df$Imported_By)
df$QAQC_By <- as.character(df$QAQC_By) 
df$StormSampleN <- as.character(df$StormSampleN)

### Unique ID number
df$UniqueID <- paste(df$Location, format(df$DateTimeET, format = "%Y-%m-%d %H:%M"), params$ParameterAbbreviation[match(df$Parameter, params$ParameterName)], sep = "_")

#############################.
#   Calculate Discharges ####
#############################.

ToCalc <- dplyr::filter(df, Location %in% ratings$MWRA_Loc[ratings$IsCurrent == TRUE], Parameter == "Staff Gauge Height")

if(nrow(ToCalc) > 0){ # If TRUE then there are discharges to be calculated
  # call function in separate script to create df of discharges and df of flags to bind to main dfs
  source(paste0(getwd(),"/src/Functions/calcDischarges.R"))
  Q_dfs <- calcQ(stages = ToCalc, userlocation = userlocation)
  # Extract the 2 dfs out of the list
  df_Q <- Q_dfs$df_Q
  df_QFlags <- Q_dfs$df_QFlags
  # df$ResultReported <- as.character(df$ResultReported)
  df <- bind_rows(df,df_Q)
  # Merge in Discharge Records
} else {
  print("No stage records available for discharge calculations")
}

###################################################.

## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df$UniqueID))
dupes <- df$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste0("This data file contains ", length(dupes),
             " records that appear to be duplicates. Eliminate all duplicates before proceeding"))
  print(dupes) # Show the duplicate Unique IDs to user in Shiny
}
### Make sure records are not already in DB

Uniq <- dbGetQuery(con, glue("SELECT UniqueID, ID FROM {database}.{schema}.{ImportTable}"))

dupes2 <- Uniq$UniqueID[Uniq$UniqueID %in% df$UniqueID]

if (length(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste0("This data file contains ", length(dupes2),
              " records that appear to already exist in the database! Eliminate all duplicates before proceeding"))
  print(dupes2) # Show the duplicate Unique IDs to user in Shiny
}
rm(Uniq)

### DataSource ####
df$DataSource <- file

### DataSourceID ####
# Do some sorting first:
df <- df[with(df, order(DateTimeET, Location, Parameter)),]

# Assign the numbers
df$DataSourceID <- seq(1, nrow(df), 1)
df$FinalResult <- round(df$FinalResult, digits = 4)
df$ImportDate <- today() %>% force_tz("America/New_York")
df$Imported_By <- username
df$FlagCode <- NA_integer_ ### There can't be any autogenerated flags 

### IDs ####
setIDs <- function(){
  query.wq <- dbGetQuery(con, glue("SELECT max(ID) FROM {database}.{schema}.{ImportTable}"))
  # Get current max ID
  if(is.na(query.wq)) {
    query.wq <- 0
  } else {
    query.wq <- query.wq
  }
  ID.max.wq <- as.numeric(unlist(query.wq))
  rm(query.wq)
  
  ### ID wq
  df$ID <- seq.int(nrow(df)) + ID.max.wq
}
df$ID <- setIDs()

# Flags

# First make sure there are flags in the dataset
setFlagIDs <- function(){
  if(all(is.na(df$FlagCode)) == FALSE){ # Condition returns FALSE if there is at least 1 non-NA value, if so proceed
    # Split the flags into a separate df and assign new ID
    df.flags <- as.data.frame(select(df,c("ID","FlagCode"))) %>%
    rename("SampleID" = ID) %>%
      drop_na()
    fc <- 1
  } else {
    df.flags <- NA
    fc <- 0
  }
  # Get discharge flags (if any)
  #### Need to deal with condition where there are no regular flags in df, but there are discharge flags
  #### This part needs to go above the SET ID function 
  if(nrow(ToCalc) > 0){
    if(!is.na(df_QFlags)){
      df_QFlags <-  df_QFlags %>%
        mutate(SampleID = df$ID[match(df_QFlags$UNQID,df$UniqueID)]) %>%
        select(-UNQID)
      fc <- fc + 2
    }
  }
  
  if(fc == 1){
    df.flags <- df.flags
  } else {
    if(fc == 2){
      rm(df.flags)
      df.flags <- as.data.frame(df_QFlags)
    } else {
    
    if(fc == 3){
      df.flags <- bind_rows(df.flags,df_QFlags)
    } else {
      df.flags <- NULL
    }
  }}
  
  
  if(class(df.flags) == "data.frame"){
    query.flags <- dbGetQuery(con, glue("SELECT max(ID) FROM {database}.{schema}.{ImportFlagTable}"))
    # Get current max ID
    if(is.na(query.flags)) {
      query.flags <- 0
    } else {
      query.flags <- query.flags
    }
    ID.max.flags <- as.numeric(unlist(query.flags))
    rm(query.flags)
    
    ### ID flags
    df.flags$ID <- seq.int(nrow(df.flags)) + ID.max.flags
    df.flags$DataTableName <- ImportTable
    df.flags$DateFlagged <-  Sys.Date() %>% as_date() %>% force_tz("America/New_York")
    df.flags$ImportStaff <-  username
    df.flags$Comment <- "Flag automatically added at import"
    
    # Reorder df.flags columns to match the database table exactly # Add code to Skip if no df.flags
    flag.col.order.wq <- dbListFields(con, schema_name = schema, name = ImportFlagTable)
    df.flags <-  df.flags[,flag.col.order.wq]
  } else { # Condition TRUE - All FlagCodes are NA, thus no df.flags needed, assign NA
    df.flags <- NA
  } # End flags processing chunk
} # End set flags function
df.flags <- setFlagIDs()

# Deselect Columns that do not need in Database
df <- df %>% select(-FlagCode)

# Verify column order of final dataframe
if(identical(names(df), cols)) {
  print("Dataframe columns match...ready to proceed with import ...")
} else {
  stop("Columns of the dataframe do not match data")
}

### QC Test ####
source(paste0(getwd(),"/src/Functions/WITQCTEST.R"))
qc_message <- QCCHECK(df.qccheck = df, 
                      file = file,
                      ImportTable = ImportTable)
print(qc_message)

# Create a list of the processed datasets
dfs <- list()
dfs[[1]] <- df
dfs[[2]] <- filepath
dfs[[3]] <- df.flags
# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION

#### COMMENT OUT WHEN RUNNING SHINY ##############
#
            # # #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
            # dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL)
            # # # Extract each element needed
            # df     <- dfs[[1]]
            # path  <- dfs[[2]]
            # df.flags  <- dfs[[3]]
##################################################.

############################.
# Write data to Database ####
############################.
# NOTE: file is just the filename, path = full path to file
IMPORT_DATA <- function(df.wq, df.flags = NULL , path, file, filename.db , processedfolder, ImportTable, ImportFlagTable = NULL){
  start <- now()
  print(glue("Starting data import at {start}"))
  ### Connect to Database   
  dsn <- "DCR_DWSP_App_R"
  database <- "DCR_DWSP"
  schema <- 'Wachusett'
  tz <- 'America/New_York'
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)
 
  odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportTable}")), value = df.wq, append = TRUE)

  ### Move Field Parameter csv files to the processed data folder ####
  print("Moving staged field parameter csv files to the imported folder...")
  # Move the raw data file to the processed folder ####
  processed_subdir <- paste0("/", max(year(df.wq$DateTimeET))) # Raw data archived by year, subfolders = Year
  processed_dir <- paste0(processedfolder, processed_subdir)
  dir.create(processed_dir)
  file.rename(path, paste0(processed_dir,"/", file))
  
    # Flag data
  if (class(df.flags) == "data.frame"){ # Check and make sure there is flag data to import 
    odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportFlagTable}")), value = df.flags, append = TRUE)
  } else {
    print("There were no flags to import")
  }
  
  # Disconnect from db and remove connection obj
  dbDisconnect(con)
  rm(con)
  
  end <- now()
  return(print(glue("Import finished at {end}, \n elapsed time {round(end - start)} seconds")))  
 }


# IMPORT_DATA(df, df.flags, path, file, filename.db, processedfolder = NULL, ImportTable, ImportFlagTable = NULL)
### END
 