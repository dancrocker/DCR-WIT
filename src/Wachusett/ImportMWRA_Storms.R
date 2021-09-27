################################  HEADER  ####################################
#  TITLE: ImportMWRA_Storms.R
#  DESCRIPTION: This script will Format/Process MWRA Storm Sample data to WQ Database
#  AUTHOR(S): Dan Crocker
#  DATE LAST UPDATED: 2020-01-17
#  GIT REPO: WIT
#  R version 3.5.3 (2019-03-11)  i386
##############################################################################.

# This script will process and import MWRA Projects: WATMDC and WATBMP

# NOTE - THIS SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

### Load libraries needed ####

    # library(tidyverse)
    # library(stringr)
    # library(odbc)
    # library(RODBC)
    # library(DBI)
    # library(lubridate)
    # library(magrittr)
    # library(readxl)
    # library(tidyverse)
    # library(stringr)
    # library(odbc)
    # library(RODBC)
    # library(DBI)
    # library(lubridate)
    # library(magrittr)
    # library(readxl)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!
########################################################################.
###                       PROCESSING FUNCTION                       ####                               
########################################################################.

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)
  # probe is an optional argument
options(scipen = 999) # Eliminate Scientific notation in numerical fields
# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)

# Read in the data to a dataframe
df.wq <- read_excel(path, sheet= 1, col_names = T, trim_ws = T, na = "nil") %>%
  as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work
df.wq <- df.wq[,c(1:26)]

########################################################################.
###                       Perform Data checks                       ####
########################################################################.

# At this point there could be a number of checks to make sure data is valid
  # Check to make sure there are 25 variables (columns)
  if (ncol(df.wq) != 26) {
    # Send warning message to UI if TRUE
    stop("There are not 26 columns of data in this file. \nMake sure the 'StormSampleN' column was added and filled in.")
  }
  # Check to make sure column 1 is "Original Sample" or other?
  if (any(colnames(df.wq)[1] != "Original Sample" & df.wq[25] != "X Ldl" & df.wq[26] != "StormSampleN")) {
    # Send warning message to UI if TRUE
    stop("At least 1 column heading is unexpected.\n Check the file before proceeding")
  }
  # Check to see if there were any miscellaneous locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Name, "WACHUSET-MISC"), TRUE)) > 0) {
  #Send warning message to UI if TRUE
    warning("There are unspecified (MISC) - please review before importing data!")
  }
  # Check to see if there were any GENERAL locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Name, "GENERAL-GEN"), TRUE)) > 0) {
    # Send warning message to UI if TRUE
    stop("There are unspecified (GEN) locations that need to be corrected before importing data")
  }
# Any other checks?  Otherwise data is validated, proceed to reformatting...

### Connect to Database   
dsn <- filename.db
database <- "DCR_DWSP"
schema <- "Wachusett"
tz <- 'America/New_York'
con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)

########################################################################.
###                     START REFORMATTING THE DATA                 ####
########################################################################.

### Rename Columns in Raw Data
names(df.wq) <- c("SampleGroup",
                  "SampleNumber",
                  "TextID",
                  "Location",
                  "Description",
                  "TripNum",
                  "LabRecDateET",
                  "SampleDate",
                  "SampleTime",
                  "PrepOnET",
                  "DateTimeAnalyzedET",
                  "AnalyzedBy",
                  "Analysis",
                  "ReportedName",
                  "Parameter",
                  "ResultReported",
                  "Units",
                  "Comment",
                  "SampledBy",
                  "Status",
                  "EDEP_Confirm",
                  "EDEP_MW_Confirm",
                  "Reportable",
                  "Method",
                  "DetectionLimit",
                  "StormSampleN")


### Date and Time ####

# Split the Sample time into date and time
df.wq$SampleDate <- as.Date(df.wq$SampleDate)
#df.wq$SampleTime[is.na(df.wq$SampleTime)] <- paste(df.wq$SampleDate[is.na(df.wq$SampleTime)])

df.wq <- separate(df.wq, SampleTime, into = c("date", "Time"), sep = " ")

# Merge the actual date column with the new Time Column and reformat to POSIXct
df.wq$DateTimeET <- as.POSIXct(paste(as.Date(df.wq$SampleDate, format ="%Y-%m-%d"), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = T)

### Fix other data types ####
df.wq$EDEP_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$EDEP_MW_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$Comment <- as.character(df.wq$Comment)
df.wq$ResultReported <- as.character(df.wq$ResultReported)

### Fix the Parameter names - from MWRA name to ParameterName ####
params <- dbReadTable(con,  Id(schema = schema, table = "tblParameters"))
df.wq$Parameter <- params$ParameterName[match(df.wq$Parameter, params$ParameterMWRAName)]

### Remove problematic Records ####

### Delete possible Sample Address rows (Associated with MISC Sample Locations):
df.wq <- df.wq %>%  # Filter out any sample with no results (There shouldn't be, but they do get included sometimes)
  filter(!is.na(Parameter),
         !is.na(ResultReported))

df.wq <- df.wq %>% slice(which(!grepl("Sample Address", df.wq$Parameter, fixed = TRUE)))
df.wq <- df.wq %>% slice(which(!grepl("(DEP)", df.wq$Parameter, fixed = TRUE))) # Filter out rows where Parameter contains  "(DEP)"
df.wq <- df.wq %>% slice(which(!grepl("X", df.wq$Status, fixed = TRUE))) # Filter out records where Status is X

# Need to generate a warning here if any status is X  - which means the results were tossed out and not approved... 

### Fix the Location names ####
df.wq$Location %<>%
  gsub("WACHUSET-","", .) %>%
  gsub("M754","MD75.4", .) %>% 
  gsub("BMP1","FPRN", .) %>%
  gsub("BMP2","FHLN", .)



### DataSource ####
df.wq <- df.wq %>% mutate(DataSource = paste0("MWRA_", file))

# note: some reported results are "A" for (DEP). These will be NA in the ResultFinal Columns
# ResultReported -
# Find all valid results, change to numeric and round to 6 digits in order to eliminate scientific notation
# Replace those results with the updated value converted back to character

edits <- str_detect(df.wq$ResultReported, paste(c("<",">"), collapse = '|')) %>%
  which(T)
update <- as.numeric(df.wq$ResultReported[-edits], digits = 6)
df.wq$ResultReported[-edits] <- as.character(update)

# Add new column for censored data
df.wq <- df.wq %>%
  mutate("IsCensored" = NA_integer_)

if(length(edits) == 0) {
  df.wq$IsCensored <- FALSE
} else {
  df.wq$IsCensored[edits] <- TRUE
  df.wq$IsCensored[-edits] <- FALSE
}

### FinalResult (numeric) ####
# Make the variable
df.wq$FinalResult <- NA
# Set the vector for mapply to operate on
x <- df.wq$ResultReported
# Function to determine FinalResult
FR <- function(x) {
  if(str_detect(x, "<")){# BDL
    as.numeric(gsub("<","", x), digits =4) # THEN strip "<" from reported result, make numeric.
  } else if (str_detect(x, ">")){
    as.numeric(gsub(">","", x)) # THEN strip ">" form reported result, make numeric.
  } else {
    as.numeric(x)
  }# ELSE THEN just use Result Reported for Result and make numeric
}
df.wq$FinalResult <- mapply(FR,x) %>%
  round(digits = 4)

########################################################################.
###                Get Discharges From HOBO Data                    ####                  
########################################################################.

### Connect in UTC timezone, times come back in local timezone
tz <- 'UTC'
con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)

locs <- df.wq$Location %>% unique()

### Placeholder for fetching USGS Instantaneous Values from NWIS ####

times <- df.wq$DateTimeET %>% with_tz("UTC") %>% unique() # convert to UTC in order to make query in db

### Pull hobo recs with matching timestamps
qry_hobo_rec <- paste0("SELECT * FROM [", schema ,"].[tbl_HOBO] WHERE [Location] IN (", paste0("'",locs,"'", collapse = ","),") AND [DateTimeUTC] IN (", paste0("'", times,"'", collapse = ","), ")") 
hobo_rec <- dbGetQuery(con, qry_hobo_rec)

dbDisconnect(con)
rm(con)

if(any(!times %in% unique(hobo_rec$DateTimeUTC))) {
  print(times[!which(times %in% hobo_rec$DateTimeUTC)] %>% with_tz("America/New_York"))
  stop("Not all storm samples have associated field parameters records (including discharge)!")
}

### Match by Location and Time
# Get Sample Loc-Times 
LocTimes <- paste0(df.wq$Location,"-", df.wq$DateTimeET)
df.wq$LocTimes <- LocTimes
### Convert hobo timestamps to local time
hobo_rec$DateTimeUTC <- with_tz(hobo_rec$DateTimeUTC, "America/New_York") 

hobo_rec <- rename(hobo_rec, "DateTimeET" = "DateTimeUTC")
hobo_locTimes <- paste0(hobo_rec$Location,"-",hobo_rec$DateTimeET)              

### Make sure all LocTimes match
if(any(!LocTimes %in% hobo_locTimes)) {
  print(LocTimes[which(LocTimes %in% hobo_locTimes)])
  stop("Not all storm samples have associated HOBO records (including discharge)!")
}
### Filter out any extraneous HOBO data records
hobo_rec <- hobo_rec[which(hobo_locTimes %in% LocTimes),]

### Format HOBO records

hobo_rec <- hobo_rec %>% 
  select(c(2,3,6:7)) %>% # Temperature not selected since it comes in with field parameters also, would cause dups if included
  rename("Staff Gauge Height" = "Stage_ft", 
         "Discharge" = "Discharge_cfs") %>% 
  gather(key = "Parameter", value = "FinalResult", 3:4) %>% 
  mutate("Units" = params$ParameterUnits[match(.$Parameter, params$ParameterName)],
         "LocTime" = paste0(.$Location, "-", .$DateTimeET))
### Add StormID
hobo_rec <- hobo_rec %>% 
  mutate("StormSampleN" = df.wq$StormSampleN[match(.$LocTime, df.wq$LocTimes)],
         "DataSource" = "DWSP_HOBO_Data",
         "ResultReported" = as.character(FinalResult)) %>% 
  select(-LocTime)

########################################################################.
###                         Get Field Parameters                    ####
########################################################################.

# Connect to db for queries below
tz <- 'UTC'
con <- dbConnect(odbc::odbc(), database, uid = database, pwd = config[35], timezone = tz)
# Below 2 lines same as above (can be deleted)
# locs <- df.wq$Location %>% unique()
# times <- df.wq$DateTimeET %>% with_tz("UTC") %>% unique() # convert to UTC in order to make query in db

### Pull hobo recs with matching timestamps
qry_fp_rec <- paste0("SELECT * FROM [Wachusett].[tblStormFieldParameters] WHERE [Location] IN (", paste0("'",locs,"'", collapse = ","),") AND DateTimeUTC IN (", paste0("#",times,"#", collapse = ","), ")")
fp_rec <- dbGetQuery(con, qry_fp_rec)

if(any(!times %in% unique(fp_rec$DateTimeUTC))) {
  print(times[which(!times %in% fp_rec$DateTimeUTC)] %>% with_tz("America/New_York")) %>% sort()
  stop("Not all storm samples have associated field parameters records!")
}

### Match by Location and Time
# Get Sample Loc-Times 
# LocTimes <- paste0(df.wq$Location,"-",df.wq$DateTimeET)

### Convert field parameter timestamps to local time
fp_rec$DateTimeUTC <- with_tz(fp_rec$DateTimeUTC, "America/New_York") 

fp_rec <- rename(fp_rec, "DateTimeET" = "DateTimeUTC")
fp_locTimes <- paste0(fp_rec$Location,"-",fp_rec$DateTimeET)              

### Make sure all LocTimes match
if(any(!LocTimes %in% fp_locTimes)) {
  print(LocTimes[which(LocTimes %in% fp_locTimes)])
  stop("Not all storm samples have associated field parameters records (including discharge)!")
}
### Filter out any extraneous field parameter records
fp_rec <- fp_rec[which(fp_locTimes %in% LocTimes),]

### Format FP records

fp_rec <- fp_rec %>% 
  select(c(2:8)) %>% 
  rename("Water Temperature" = "Water_temp_c", 
          "Specific Conductance" = "Conductivity_uScm",
          "Dissolved Oxygen" = "Dissolved_Oxygen_mgL") %>% 
  gather(key = "Parameter", value = "FinalResult", 4:7) %>% 
  mutate("Units" = params$ParameterUnits[match(.$Parameter, params$ParameterName)],
         "DataSource" = "DWSP_YSI_Storm_Field_Data",
         "ResultReported" = as.character(FinalResult)) %>% 
  rename("StormSampleN" = StormID)

########################################################################.
###                  Combine HOBO and FP records with df.wq         ####
########################################################################.

df.wq <- df.wq %>% 
  bind_rows(hobo_rec) %>% 
  bind_rows(fp_rec)

df.wq$LocTime <-  paste0(df.wq$Location,"-",df.wq$DateTimeET)  

### Update the storm sample numbers ####
for (loc in unique(df.wq$Location)){
  times <- df.wq$DateTimeET[df.wq$Location == loc] %>% unique() %>% sort()
  for (i in seq_along(times)) {
    df.wq$StormSampleN[df.wq$Location == loc & df.wq$DateTimeET == times[i]] <- paste0(df.wq$StormSampleN[df.wq$Location == loc & df.wq$DateTimeET == times[i]], "-", i)
  }
}

########################################################################.
###                         Add Unique ID                       ####
########################################################################.

### Unique ID number ####
df.wq$UniqueID <- NA_character_
df.wq$UniqueID <- paste(df.wq$Location, format(df.wq$DateTimeET, format = "%Y-%m-%d %H:%M"), params$ParameterAbbreviation[match(df.wq$Parameter, params$ParameterName)], sep = "_")

########################################################################.
###                        Check for Duplicates                     ####
########################################################################. 

## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df.wq$UniqueID))
dupes <- df.wq$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste("This data file contains", length(dupes),
             "records that appear to be duplicates. Eliminate all duplicates before proceeding.",
             "The duplicate records include:", paste(head(dupes, 15), collapse = ", ")), call. = FALSE)
}
### Make sure records are not already in DB ####
Uniq <- dbGetQuery(con, glue("SELECT [UniqueID], [ID] FROM [{schema}].[{ImportTable}]"))
flags <- dbGetQuery(con, glue("SELECT [SampleID], [FlagCode] FROM [{schema}].[{ImportFlayTable}] WHERE FlagCode = 102"))
dupes2 <- Uniq[Uniq$UniqueID %in% df.wq$UniqueID,]
dupes2 <- filter(dupes2, !ID %in% flags$SampleID) # take out any preliminary samples (they should get overwritten during import)

if (nrow(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste("This data file contains", nrow(dupes2),
             "records that appear to already exist in the database!
Eliminate all duplicates before proceeding.",
             "The duplicate records include:", paste(head(dupes2$UniqueID, 15), collapse = ", ")), call. = FALSE)
}

rm(Uniq)

### DataSourceID ####

# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(DateTimeET, Location, Parameter)),]

# Assign the numbers
df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)

### Flag (numeric) ####
# Use similar function as to assign flags
df.wq$FlagCode <- NA
x <- df.wq$ResultReported
FLAG <- function(x) {
  if (str_detect(x, "<")) {
    100     # THEN set BDL (100 for all datasets except reservoir nutrients)
  } else if (str_detect(x, ">")){
      101     # THEN set to 101 for ADL
    } else {
      NA
    }
}
df.wq$FlagCode <- mapply(FLAG,x) %>% as.numeric()

### Storm SampleN (numeric) - brought in during bind_rows operation ####

### Importdate (Date) ####
df.wq$ImportDate <- Sys.Date()

### IDs ####

# Read Tables
# WQ
setIDs <- function(){
query.wq <- dbGetQuery(con, glue("SELECT max(ID) FROM [{schema}].[{ImportTable}]"))
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

########################################################################.
###                           Flags                                 ####
########################################################################.

# First make sure there are flags in the dataset
setFlagIDs <- function(){
  if(all(is.na(df.wq$FlagCode)) == FALSE){ # Condition returns FALSE if there is at least 1 non-NA value, if so proceed
  # Split the flags into a separate df and assign new ID
  df.flags <- as.data.frame(select(df.wq,c("ID","FlagCode"))) %>%
    rename("SampleID" = ID) %>%
    drop_na()
  fc <- 1 # Flag Count
  } else {
    df.flags <- NA
    fc <- 0
  }
  ### NOTE: Flags for discharges above/below rating curve not generated here - only in HOBOB data ####
  
  # Get discharge flags (if any)
  #### Need to deal with condition where there are no regular flags in df.wq, but there are discharge flags
  #### This part needs to go above the SET ID function 
  # if(nrow(ToCalc) > 0){
  #     if(!is.na(df_QFlags)){
  #       df_QFlags <-  df_QFlags %>%
  #         mutate(SampleID = df.wq$ID[match(df_QFlags$UNQID,df.wq$UniqueID)]) %>%
  #         select(-UNQID)
  #       fc <- fc + 2
  #     }
  # }
  if(fc == 1){
    df.flags <- df.flags
  } else {
    if(fc == 3){
      df.flags <- bind_rows(df.flags,df_QFlags)
    } else {
      df.flags <- NA
    }
  }
      
  if(class(df.flags) == "data.frame"){
    query.flags <- dbGetQuery(con, glue("SELECT max(ID) FROM [{schema}].[{ImportFlagTable}]"))
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
      df.flags$DateFlagged <-  Sys.Date()
      df.flags$ImportStaff <-  username
    
      # Reorder df.flags columns to match the database table exactly # Add code to Skip if no df.flags
      df.flags <- df.flags[,c(3,4,1,2,5,6)]
  } else { # Condition TRUE - All FlagCodes are NA, thus no df.flags needed, assign NA
    df.flags <- NA
  } # End flags processing chunk
} # End set flags function
df.flags <- setFlagIDs()


### Check for sample location/time combination already in database. Create dataframe for records with no matches (possible time fix needed)

#Create empty dataframe
# unmatchedtimes <- df.wq[NULL,]
# # Bring in tributary location IDs
# locations.tribs <- na.omit(dbGetQuery(con, "SELECT LocationMWRA FROM tblLocations WHERE LocationType ='Tributary'"))
# # Keep only locations of type "Tributary"
# df.timecheck <- dplyr::filter(df.wq, Location %in% locations.tribs$LocationMWRA)
# rm(locations.tribs)
# 
# # Only do the rest of the unmatched times check if there are tributary locations in data being processed
# if(nrow(df.timecheck)>0){
# 
#   # Find earliest date in df.wq
#   mindatecheck <- min(df.wq$DateTimeET)
#   # Retrieve all date/times from database from earliest in df.wq to present
#   databasetimes <- dbGetQuery(con, paste0("SELECT DateTimeET, Location FROM ", ImportTable," WHERE DateTimeET >= #",mindatecheck,"#"))  
#     
#   #Loop adds row for every record without matching location/date/time in database
#   for (i in 1:nrow(df.timecheck)){
#     if ((df.timecheck$DateTimeET[i] %in% dplyr::filter(databasetimes,Location==df.timecheck$Location[i])$DateTimeET) == FALSE){
#       unmatchedtimes <- bind_rows(unmatchedtimes,df.timecheck[i,])
#    }}
# 
#   rm(mindatecheck, databasetimes)
# 
# }  
#   
# ### Print unmatchedtimes to log, if present
# if (nrow(unmatchedtimes)>0){
#   print(paste0(nrow(unmatchedtimes)," unmatched site/date/times in processed data."))
#   print(unmatchedtimes[c("ID","UniqueID","ResultReported")],print.gap=4,right=FALSE)
# }

# Reformatting 2 ####

### Deselect Columns that do not need in Database
df.wq <- df.wq %>% select(-c(Description,
                             LocTimes,
                             SampleDate,
                             FlagCode,
                             date,
                             Time
                             )
)

# Reorder remaining 30 columns to match the database table exactly
col.order.wq <- dbListFields(con, schema_name = schema, name = ImportTable)
df.wq <-  df.wq[,col.order.wq]

# QC Test
source(paste0(getwd(),"/src/Functions/WITQCTEST.R"))
qc_message <- QCCHECK(df.qccheck=df.wq,file=file,ImportTable=ImportTable)
print(qc_message)

# Create a list of the processed datasets
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- path
dfs[[3]] <- df.flags # Removed condition to test for flags and put it in the setFlagIDS() function
dfs[[4]] <- unmatchedtimes # Samples with site/time combo not matching any record in the database

# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION

#### COMMENT OUT SECTION BELOW WHEN RUNNING SHINY
########################################################################################################.
# #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = ImportFlagTable)
# #
# # # Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]
# unmatchedtimes <- dfs[[4]]

########################################################################################################.
##########################.
# Write data to Database #
##########################.

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
  start <- now()
  print(glue("Starting data import at {start}"))
  ### Connect to Database   
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- "Wachusett"
  tz <- 'America/New_York'
  con <- dbConnect(odbc::odbc(), dsn, uid = dsn, pwd = config[35], timezone = tz)

  odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportTable}")), value = df.wq, append = TRUE)

  ### Flag data ####
  if (class(df.flags) == "data.frame"){ # Check and make sure there is flag data to import 
    odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportFlagTable}")), value = df.flags, append = TRUE)
  } else {
    print("There were no flags to import")
  }

  # Disconnect from db and remove connection obj
  dbDisconnect(con)
  rm(con)

  ### Move the processed raw data file to the processed folder ####
  processed_subdir <- paste0("/", max(year(df.wq$DateTimeET))) # Raw data archived by year, subfolders = Year
  dir.create(processed_subdir)
  processed_dir <- paste0(processedfolder, processed_subdir)
  file.rename(path, paste0(processed_dir,"/", file))
  
  end <- now()
  return(print(glue("Import finished at {end}, \n elapsed time {round(end - start)} seconds")))  
}
### END ####

# IMPORT_DATA(df.wq, df.flags, path, file, filename.db, processedfolder)
