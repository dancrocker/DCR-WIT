###############################  HEADER  ######################################
#  TITLE: ImportMWRA.R
#  DESCRIPTION: This script will Format/Process MWRA data to DCR
#               This script will process and import MWRA Projects: WATTRB, WATTRN, MDCMNTH, WATBMP, QUABBIN, MDHEML
#  AUTHOR(S): Dan Crocker
#  DATE LAST UPDATED: 2020-01-21
#  GIT REPO: WIT
#  R version 3.5.3 (2019-03-11)  i386
##############################################################################.

# NOTE - THIS SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

# # Load libraries needed ####

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
    # library(testthat)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

########################################################################.
###                         PROCESSING FUNCTION                     ####
########################################################################.

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)
  # probe is an optional argument
options(scipen = 999) # Eliminate Scientific notation in numerical fields
# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)

# Read in the data to a dataframe
df.wq <- read_excel(path, sheet = 1, col_names = T, trim_ws = T, na = "nil") %>%
  as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work
df.wq <- df.wq[,c(1:25)]

########################################################################.
###                        Perform Data checks                      ####
########################################################################.

# At this point there could be a number of checks to make sure data is valid

  # Check to make sure there are 25 variables (columns)
  if (ncol(df.wq) != 25) {
    # Send warning message to UI if TRUE
    stop("There are not 25 columns of data in this file.\n Check the file before proceeding")
  }
  # Check to make sure column 1 is "Original Sample" or other?
  if (any(colnames(df.wq)[1] != "Original Sample" & df.wq[25] != "X Ldl")) {
    # Send warning message to UI if TRUE
    stop("At least 1 column heading is unexpected.\n Check the file before proceeding")
  }
  # Check to see if there were any miscellaneous locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Name, "WACHUSET-MISC"),TRUE)) > 0) {
  #Send warning message to UI if TRUE
    warning("There are unspecified (MISC) - please review before importing data!")
  }
  # Check to see if there were any GENERAL locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Name, "GENERAL-GEN"),TRUE)) > 0) {
    # Send warning message to UI if TRUE
    stop("There are unspecified (GEN) locations that need to be corrected before importing data")
  }
# Any other checks?  Otherwise data is validated, proceed to reformatting...

# Connect to db for queries below
con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}",
                                            paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                 timezone = "America/New_York")

########################################################################.
###                     START REFORMATTING THE DATA                 ####
########################################################################.

### Rename Columns in Raw Data ####
names(df.wq) = c("SampleGroup",
                 "SampleNumber",
                 "TextID",
                 "Location",
                 "Description",
                 "TripNum",
                 "LabRecDate",
                 "SampleDate",
                 "SampleTime",
                 "PrepOn",
                 "DateTimeAnalyzed",
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
                 "DetectionLimit")

# Check to make sure there is not storm sample data in the dataset (> 1 Loc/Date combination)
if (nrow(df.wq) != length(paste0(df.wq$SampleDate, df.wq$Location,df.wq$Parameter) %>% unique())) {
  # Send warning message to UI that it appears that there are storm samples in the data
  stop("There seems to be storm sample data in this file.\n There are more than 1 result for a parameter on a single day. 
         \nCheck the file before proceeding and split storm samples into separate file to import")
}

### Date and Time ####

# Split the Sample time into date and time
df.wq$SampleDate <- as.Date(df.wq$SampleDate)
#df.wq$SampleTime[is.na(df.wq$SampleTime)] <- paste(df.wq$SampleDate[is.na(df.wq$SampleTime)])

df.wq <- separate(df.wq, SampleTime, into = c("date", "Time"), sep = " ")

# Merge the actual date column with the new Time Column and reformat to POSIXct
df.wq$SampleDateTime <- as.POSIXct(paste(as.Date(df.wq$SampleDate, format ="%Y-%m-%d"), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = T)

### Fix other data types ####
df.wq$EDEP_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$EDEP_MW_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$Comment <- as.character(df.wq$Comment)
df.wq$ResultReported <- as.character(df.wq$ResultReported)

### Fix the Parameter names ####  - change from MWRA name to ParameterName
params <- dbReadTable(con,"tblParameters")
df.wq$Parameter <- params$ParameterName[match(df.wq$Parameter, params$ParameterMWRAName)]

### Remove records with missing elements/uneeded data ####
# Delete possible Sample Address rows (Associated with MISC Sample Locations):
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
  gsub("BMP1","PRNW", .) %>%
  gsub("BMP2","HLNW", .) %>%
  gsub("QUABBINT-","", .) %>%
  gsub("QUABBIN-","", .)

########################################################################.
###                           Add new Columns                       ####
########################################################################.

### Unique ID number ####
df.wq$UniqueID <- ""
df.wq$UniqueID <- paste(df.wq$Location, format(df.wq$SampleDateTime, format = "%Y-%m-%d %H:%M"), params$ParameterAbbreviation[match(df.wq$Parameter, params$ParameterName)], sep = "_")

########################################################################.
###                         Calculate Discharges                    ####
########################################################################.

ratings <- dbReadTable(con, "tblRatings")
ToCalc <- filter(df.wq, Location %in% ratings$MWRA_Loc[ratings$Current == TRUE], Parameter == "Staff Gauge Height")
if(nrow(ToCalc) > 0){ # If TRUE then there are discharges to be calculated
  # call function in separate script to create df of discharges and df of flags to bind to main dfs
  source(paste0(getwd(),"/src/Functions/calcDischarges.R"))
  Q_dfs <- calcQ(filename.db = filename.db, stages = ToCalc)
  # Extract the 2 dfs out of the list
  df_Q <- Q_dfs$df_Q
  df_QFlags <- Q_dfs$df_QFlags
  df.wq <- bind_rows(df.wq,df_Q)
  # Merge in Discharge Records
} else {
  print("No stage records available for discharge calculations")
}

########################################################################.
###                           Check Duplicates                      ####
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

Uniq <- dbGetQuery(con, paste0("SELECT UniqueID, ID FROM ", ImportTable))
flags <- dbGetQuery(con, paste0("SELECT SampleID, FlagCode FROM ", ImportFlagTable," WHERE FlagCode = 102"))
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

### DataSource ####
df.wq <- df.wq %>% mutate(DataSource = paste0("MWRA_", file))

### DataSourceID ####
# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(SampleDateTime, Location, Parameter)),]

# Assign the numbers
df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)

# note: some reported results are "A" for (DEP). These will be NA in the ResultFinal Columns
# ResultReported -
# Find all valid results, change to numeric and round to 6 digits in order to eliminate scientific notation
# Replace those results with the updated value converted back to character

edits <- str_detect(df.wq$ResultReported, paste(c("<",">"), collapse = '|')) %>%
  which(T)
update <- as.numeric(df.wq$ResultReported[-edits], digits = 6)
df.wq$ResultReported[-edits] <- as.character(update)

### FinalResult (numeric)
# Make the variable
df.wq$FinalResult <- NA
# Set the vector for mapply to operate on
x <- df.wq$ResultReported
# Function to determine FinalResult
FR <- function(x) {
  if(str_detect(x, "<")){# BDL
    as.numeric(gsub("<","", x), digits =4) * 0.5 # THEN strip "<" from reported result, make numeric, divide by 2.
  } else if (str_detect(x, ">")){
      as.numeric(gsub(">","", x)) # THEN strip ">" form reported result, make numeric.
    } else {
      as.numeric(x)
    }# ELSE THEN just use Result Reported for Result and make numeric
  }
df.wq$FinalResult <- mapply(FR,x) %>%
  round(digits = 4)

### Flag (numeric) ####
# Use similar function as to assign flags
df.wq$FlagCode <- NA
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

### Storm SampleN (numeric) ####
df.wq$StormSampleN <- NA_character_

### Import date (Date) ####
df.wq$ImportDate <- Sys.Date()

########################################################################.
###                      REMOVE ANY PRELIMINARY DATA                ####
########################################################################.

# Calculate the date range of import ####
datemin <- min(df.wq$SampleDateTime)
datemax <- max(df.wq$SampleDateTime)

# IDs to look for - all records in time peroid in question
qry <- paste0("SELECT (ID) FROM ", ImportTable, " WHERE (SampleDateTime) >= #", datemin, "# AND (SampleDateTime) <= #", datemax,"#")
query.prelim <- dbGetQuery(con, qry) # This generates a list of possible IDs

if (nrow(query.prelim) > 0) {# If true there is at least one record in the time range of the data
  # SQL query that finds matching sample ID from tblSampleFlagIndex Flagged 102 within date range in question
  qryS <- paste0("SELECT SampleID FROM ", ImportFlagTable, " WHERE FlagCode = 102 AND SampleID IN (", paste0(query.prelim$ID, collapse = ","), ")")
  qryDelete <- dbGetQuery(con,qryS) # Check the query to see if it returns any matches
  # If there are matching records then delete preliminary data (IDs flagged 102 in period of question)
  if(nrow(qryDelete) > 0) {
    qryDeletePrelimData <- paste0("DELETE * FROM ", ImportTable," WHERE ID IN (", paste0(qryDelete$SampleID, collapse = ","), ")")
    rs <- dbSendStatement(con,qryDeletePrelimData)
    print(paste(dbGetRowsAffected(rs), "preliminary records were deleted during this import", sep = " ")) # Need to display this message to the Shiny UI
    dbClearResult(rs)

# Next delete all flags associated with preliminary data - Will also delete any other flag associated with record number
    qryDeletePrelimFlags <- paste0("DELETE * FROM ", ImportFlagTable, " WHERE SampleID IN (", paste0(qryDelete$SampleID, collapse = ","), ")")
    rs <- dbSendStatement(con, qryDeletePrelimFlags)
    print(paste(dbGetRowsAffected(rs), "preliminary record data flags were deleted during this import", sep = " "))
    dbClearResult(rs)
  }
}

########################################################################.
###                               SET IDs                           ####
########################################################################.

# Read Tables
# WQ ####
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

### Flags ####

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
  ### Get discharge flags (if any) ####
  #### Need to deal with condition where there are no regular flags in df.wq, but there are discharge flags
  #### This part needs to go above the SET ID function 
  
  if(nrow(ToCalc) > 0){
      if(!is.na(df_QFlags)){
        df_QFlags <-  df_QFlags %>%
          mutate(SampleID = df.wq$ID[match(df_QFlags$UNQID,df.wq$UniqueID)]) %>%
          select(-UNQID)
        fc <- fc + 2
      }
  }
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
      query.flags <- dbGetQuery(con, paste0("SELECT max(ID) FROM ", ImportFlagTable))
      # Get current max ID
      if(is.na(query.flags)) {
        query.flags <- 0
      } else {
        query.flags <- query.flags
      }
      ID.max.flags <- as.numeric(unlist(query.flags))
      rm(query.flags)
      
    ### ID flags ###
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

########################################################################.
###                        Check for unmatched times                ####
########################################################################.

### Check for sample location/time combination already in database. Create dataframe for records with no matches (possible time fix needed)

#Create empty dataframe
unmatchedtimes <- df.wq[NULL,names(df.wq)]
# Bring in tributary location IDs
locations.tribs <- na.omit(dbGetQuery(con, "SELECT LocationMWRA FROM tblLocations WHERE LocationType ='Tributary'"))
# Keep only locations of type "Tributary"
df.timecheck <- dplyr::filter(df.wq, Location %in% locations.tribs$LocationMWRA)
rm(locations.tribs)

# Only do the rest of the unmatched times check if there are tributary locations in data being processed
if(nrow(df.timecheck)>0){

  # Find earliest date in df.wq
  mindatecheck <- min(df.wq$SampleDateTime)
  # Retrieve all date/times from database from earliest in df.wq to present
  databasetimes <- dbGetQuery(con, paste0("SELECT SampleDateTime, Location FROM ", ImportTable," WHERE SampleDateTime >= #",mindatecheck,"#"))  
    
  #Loop adds row for every record without matching location/date/time in database
  for (i in 1:nrow(df.timecheck)){
    if ((df.timecheck$SampleDateTime[i] %in% dplyr::filter(databasetimes,Location==df.timecheck$Location[i])$SampleDateTime) == FALSE){
      unmatchedtimes <- bind_rows(unmatchedtimes,df.timecheck[i,])
   }}

  rm(mindatecheck, databasetimes)

}  
  
### Print unmatchedtimes to log, if present
if (nrow(unmatchedtimes)>0){
  print(paste0(nrow(unmatchedtimes)," unmatched site/date/times in processed data."))
  print(unmatchedtimes[c("ID","UniqueID","ResultReported")],print.gap=4,right=FALSE)
}

########################################################################.
###                           Final Reformatting                    ####
########################################################################. 

### Deselect Columns that do not need in Database ####
df.wq <- df.wq %>% select(-c(Description,
                             SampleDate,
                             FlagCode,
                             date,
                             Time
                             )
)

# Reorder remaining 30 columns to match the database table exactly ####
col.order.wq <- dbListFields(con, ImportTable)
df.wq <-  df.wq[,col.order.wq]

### QC Test ####
source(paste0(getwd(),"/src/Functions/WITQCTEST.R"))
qc_message <- QCCHECK(df.qccheck=df.wq,file=file,ImportTable=ImportTable)
print(qc_message)

### Create a list of the processed datasets ####
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- path
dfs[[3]] <- df.flags # Removed condition to test for flags and put it in the setFlagIDS() function
dfs[[4]] <- unmatchedtimes # Samples with site/time combo not matching any record in the database

# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION ####

############## COMMENT OUT SECTION BELOW WHEN RUNNING SHINY #############.
# #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = ImportFlagTable)
# #
# # # Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]
# unmatchedtimes <- dfs[[4]]

########################################################################.

########################################################################.
###                        Write data to Database                   ####
########################################################################.


IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
# df.flags is an optional argument

  con <-  odbcConnectAccess(filename.db)

  # Import the data to the database - Need to use RODBC methods here. Tried odbc and it failed

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

  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)

  ### Move the processed raw data file to the processed folder ####
  processed_subdir <- paste0("/", max(year(df.wq$SampleDateTime))) # Raw data archived by year, subfolders = Year
  processed_dir <- paste0(processedfolder, processed_subdir)
  file.rename(path, paste0(processed_dir,"/", file))
  return("Import Successful")
}
### END ####

# IMPORT_DATA(df.wq, df.flags, path, file, filename.db, processedfolder)
