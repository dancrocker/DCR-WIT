##############################################################################################################################
#     Title: ImportMWRA.R
#     Description: This script will Format/Process MWRA data to DCR
#     Written by: Dan Crocker
#     Last Update: March 2018
#    This script will process and import MWRA Projects: WATTRB, WATTRN, MDCMNTH, WATBMP, QUABBIN, MDHEML
#
##############################################################################################################################

# NOTE - THIS SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

# # Load libraries needed
#     library(tidyverse)
#     library(stringr)
#     library(odbc)
#     library(RODBC)
#     library(DBI)
#     library(lubridate)
#     library(magrittr)
#     library(readxl)
#
# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

#############################
#   PROCESSING FUNCTION    #
############################

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)
  # probe is an optional argument
options(scipen = 999) # Eliminate Scientific notation in numerical fields
# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)

# Read in the data to a dataframe
df.wq <- read_excel(path, sheet= 1, col_names = T, trim_ws = T, na = "nil") %>%
  as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work
df.wq <- df.wq[,c(1:25)]

### Perform Data checks ###

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
  # # Check to see if there were any miscellaneous locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Name, "WACHUSET-MISC"),TRUE)) > 0) {
  #Send warning message to UI if TRUE
    stop("There are unspecified (MISC) locations that need to be corrected before importing data")
  }
  # Check to see if there were any GENERAL locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Name, "GENERAL-GEN"),TRUE)) > 0) {
    # Send warning message to UI if TRUE
    stop("There are unspecified (GEN) locations that need to be corrected before importing data")
  }
# Any other checks?  Otherwise data is validated, proceed to reformatting...
###

# Connect to db for queries below
con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb, *.accdb)}",
                                            paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                 timezone = "America/New_York")

#################################
#  START REFORMATTING THE DATA  #
#################################

### Rename Columns in Raw Data
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


### Date and Time:
# SampleDateTime
# Split the Sample time into date and time
df.wq$SampleDate <- as.Date(df.wq$SampleDate)
#df.wq$SampleTime[is.na(df.wq$SampleTime)] <- paste(df.wq$SampleDate[is.na(df.wq$SampleTime)])

df.wq <- separate(df.wq, SampleTime, into = c("date", "Time"), sep = " ")

# Merge the actual date column with the new Time Column and reformat to POSIXct
df.wq$SampleDateTime <- as.POSIXct(paste(as.Date(df.wq$SampleDate, format ="%Y-%m-%d"), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = T)

# Fix other data types
df.wq$EDEP_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$EDEP_MW_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$Comment <- as.character(df.wq$Comment)
df.wq$ResultReported <- as.character(df.wq$ResultReported)

# Fix the Parameter names  - change from MWRA name to ParameterName
params <- dbReadTable(con,"tblParameters")
df.wq$Parameter <- params$ParameterName[match(df.wq$Parameter, params$ParameterMWRAName)]

# Delete possible Sample Address rows (Associated with MISC Sample Locations):
df.wq <- filter(df.wq, !is.na(ResultReported)) %>%  # Filter out any sample with no results (There shouldn't be, but they do get included sometimes)
  filter(!is.na(Parameter))
df.wq <- df.wq %>% slice(which(!grepl("Sample Address", df.wq$Parameter, fixed = TRUE)))
df.wq <- df.wq %>% slice(which(!grepl("(DEP)", df.wq$Parameter, fixed = TRUE))) # Filter out rows where Parameter contains  "(DEP)"
df.wq <- df.wq %>% slice(which(!grepl("X", df.wq$Status, fixed = TRUE))) # Filter out records where Status is X
# Fix the Location names
df.wq$Location %<>%
  gsub("WACHUSET-","", .) %>%
  gsub("BMP1","FPRN", .) %>%
  gsub("BMP2","FHLN", .) %>%
  gsub("QUABBINT-","", .) %>%
  gsub("QUABBIN-","", .)

######################
#   Add new Columns  #
######################

### Unique ID number
df.wq$UniqueID <- ""
df.wq$UniqueID <- paste(df.wq$Location, format(df.wq$SampleDateTime, format = "%Y-%m-%d %H:%M"), params$ParameterAbbreviation[match(df.wq$Parameter, params$ParameterName)], sep = "_")

###########################
#   Calculate Discharges  #
###########################

ratings <- dbReadTable(con, "tblRatings")
ToCalc <- filter(df.wq, Location %in% ratings$MWRA_Loc, Parameter == "Staff Gauge Height")
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
###########################

## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df.wq$UniqueID))
dupes <- df.wq$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste("This data file contains", length(dupes),
             "records that appear to be duplicates. Eliminate all duplicates before proceeding.",
             "The duplicate records include:", paste(head(dupes, 15), collapse = ", ")), call. = FALSE)
}
### Make sure records are not already in DB

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

### DataSource
df.wq <- df.wq %>% mutate(DataSource = paste0("MWRA_", file))

### DataSourceID
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

### Flag (numeric)
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

### Storm SampleN (numeric)
df.wq$StormSampleN <- NA %>% as.numeric

### Importdate (Date)
df.wq$ImportDate <- today()

######################################
# REMOVE ANY PRELIMINARY DATA     ###
######################################

# Calculate the date range of import
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
#####################################################################
### IDs

# Read Tables
# WQ
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
# Flags
# First make sure there are flags in the dataset
setFlagIDs <- function(){
  if (all(is.na(df.wq$FlagCode)) == FALSE){ # Condition returns FALSE if there is at least 1 non-NA value, if so proceed
  query.flags <- dbGetQuery(con, paste0("SELECT max(ID) FROM ", ImportFlagTable))
  # Get current max ID
  if(is.na(query.flags)) {
    query.flags <- 0
  } else {
    query.flags <- query.flags
  }
  ID.max.flags <- as.numeric(unlist(query.flags))
  rm(query.flags)

  # Split the flags into a separate df and assign new ID
  df.flags <- as.data.frame(select(df.wq,c("ID","FlagCode"))) %>%
    rename("SampleFlag_ID" = ID) %>%
    drop_na()
  # Get discharge flags (if any)
  if(nrow(ToCalc) > 0){
      if(!is.na(df_QFlags)){
        df_QFlags <-  df_QFlags %>%
          mutate(SampleFlag_ID = df.wq$ID[match(df_QFlags$UNQID,df.wq$UniqueID)]) %>%
          select(-UNQID)
        df.flags <- bind_rows(df.flags,df_QFlags)
      }
  }
  ### ID flags
  df.flags$ID <- seq.int(nrow(df.flags)) + ID.max.flags
  df.flags$DateFlagged = today()
  df.flags$ImportStaff = Sys.getenv("USERNAME")

  # Reorder df.flags columns to match the database table exactly # Add code to Skip if no df.flags
  df.flags <- df.flags[,c(3,1,2,4,5)]
  } else { # Condition TRUE - All FlagCodes are NA, thus no df.flags needed, assign NA
    df.flags <- NA
  } # End flags processing chunk
} # End set flags function
df.flags <- setFlagIDs()

##############################################################################################################################
# Reformatting 2
##############################################################################################################################

### Deselect Columns that do not need in Database
df.wq <- df.wq %>% select(-c(Description,
                             SampleDate,
                             FlagCode,
                             date,
                             Time
                             )
)

# Reorder remaining 30 columns to match the database table exactly
col.order.wq <- dbListFields(con, ImportTable)
df.wq <-  df.wq[,col.order.wq]

# Create a list of the processed datasets
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- path
dfs[[3]] <- df.flags # Removed condition to test for flags and put it in the setFlagIDS() function

# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION

#### COMMENT OUT SECTION BELOW WHEN RUNNING SHINY
########################################################################################################
# #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = ImportFlagTable)
#
# # Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]

########################################################################################################
##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder,ImportTable, ImportFlagTable = NULL){
# df.flags is an optional argument

  con <-  odbcConnectAccess(filename.db)

  # Import the data to the database - Need to use RODBC methods here. Tried odbc and it failed

  # WQ Data
  ColumnsOfTable <- sqlColumns(con, ImportTable)
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME)
  sqlSave(con, df.wq, tablename = ImportTable, append = T,
          rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)

  # Flag data
   if (!is.null(nrow(df.flags)) == TRUE){ # Check and make sure there is flag data to import
    sqlSave(con, df.flags, tablename = ImportFlagTable, append = T,
            rownames = F, colnames = F, addPK = F , fast = F)
  }

  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)

  #Move the processed raw data file to the processed folder
  processed_subdir <- paste0("/", max(year(df.wq$SampleDateTime))) # Raw data archived by year, subfolders = Year
  processed_dir <- paste0(processedfolder, processed_subdir)
  file.rename(path, paste0(processed_dir,"/", file))
  return("Import Successful")
}
### END

# IMPORT_DATA(df.wq, df.flags, path, file, filename.db, processedfolder)
