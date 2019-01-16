##############################################################################################################################
#     Title: ImportTurbidity.R
#     Description: This script will Format/Process MWRA data to DCR
#     Written by: Dan Crocker
#     Last Update: April 2018
#    This script will process and import Wachusett Turbidity data to the WQ Database
#
##############################################################################################################################

# NOTE - THIS SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

# Load libraries needed
#
        # library(tidyverse)
        # library(stringr)
        # library(odbc)
        # library(RODBC)
        # library(DBI)
        # library(magrittr)
        # library(openxlsx)
        # library(DescTools)
        # library(lubridate)
        # library(devtools)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

##############
# PREP DATA  #  # Function to Prep Data for use with Import Function
##############

PREP_DATA <- function(file){

  ########################################################
  # Set system environments (Future - try to set this up to be permanent)
  Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe")
  Sys.setenv(PATH = paste("C:/Rtools/bin", Sys.getenv("PATH"), sep=";"))
  Sys.setenv(BINPREF = "C:/Rtools/mingw_$(WIN)/bin/")
  # Check system environments
  # Sys.getenv("R_ZIPCMD", "zip")
  # Sys.getenv("PATH") # Rtools should be listed now
  ########################################################

wb <- file
wbobj <- loadWorkbook(wb)
# Extract the full data from the sheet
data <- openxlsx::read.xlsx(wbobj, sheet =  1, startRow = 3, colNames = F, cols = c(1:5))

if (is.null(data)){
  # Send warning message to UI
  stop("There are no records in the file! Make sure there are records and try agian")
}

# Get the data in order and formatted
    df <- data
    df$Parameter <- "Turbidity NTU" %>% as.factor()
    df$Units <- "NTU" %>% as.factor
    df$X2 <- XLDateToPOSIXct(df$X2)
    df$X3 <- XLDateToPOSIXct(df$X3)
    df <- separate(df, X3, into = c("Date", "Time"), sep = " ")
    df$SampleDateTime <- as.POSIXct(paste(as.Date(df$X2, format ="%Y-%m-%d"), df$Time, sep = " "), format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = T)
    df <- df[,-c(2:4)]
    df <- dplyr::rename(df, "Location" = X1,"FinalResult" = X4, "SampledBy" = X5)

    df$FinalResult <- as.numeric(df$FinalResult)
    df$Location <- as.factor(df$Location)
    df$SampledBy <- as.factor(df$SampledBy)

# Find the row to paste the data on sheet 2, then copy the data over
    PasteRow <- as.numeric(NROW(read.xlsx(wb, sheet =  "ImportedToWQDB", colNames = F, cols = 1)) + 1)
    openxlsx::writeData(wbobj, sheet = 2, data , startCol = 1, startRow = PasteRow, colNames = F)

  # Find the last row of data to delete on sheet 1, delete the data, then save the workbook
    EndRow <- NROW(data) + 2
    openxlsx::deleteData(wbobj, sheet = 1, cols = 1:5, rows = 3:EndRow, gridExpand = T)
    openxlsx::saveWorkbook(wbobj, wb, overwrite = TRUE)

# Copy the columns into the import template spreadsheet:
    # Open the Workbook and create a workbook object to manipulate
    wb <- config[18]
    wbobj <- loadWorkbook(wb)
    # Find the last row in the import table
    EndRow <- as.numeric(NROW(read.xlsx(wbobj, sheet = 1, colNames = T)) + 1)
    # Delete the old data
    deleteData(wbobj, sheet = 1, cols = 1:25, rows = 2:EndRow, gridExpand = T)
    # Insert the new data
    writeData(wbobj, sheet = 1, df[,1] , startCol = 4, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,2] , startCol = 16, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,3] , startCol = 19, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,4] , startCol = 15, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,5] , startCol = 17, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,6] , startCol = 8, startRow = 2, colNames = F)
    # Save the workbook and then proceed
    saveWorkbook(wbobj, wb, overwrite = TRUE)
} # End PREP DATA Function




#############################
#   PROCESSING FUNCTION    #
############################

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)

options(scipen = 999) # Eliminate Scientific notation in numerical fields
# Path to raw data file
path <- paste0(rawdatafolder,"/", file)
# Prep the raw data - send to importer worksheet
PREP_DATA(file = path)
# Set the importer worksheet path
importerpath <- config[18]
# Read in the data to a dataframe
df.wq <- read_excel(importerpath, sheet= 1, col_names = T, trim_ws = T, na = "nil") %>%
  as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work
df.wq <- df.wq[,c(1:25)]
### Perform Data checks ###

# At this point there could be a number of checks to make sure data is valid
  # Check to make sure there are 25 variables (columns)
  if (ncol(df.wq) != 25) {
    # Send warning message to UI
    #warning1 <- print(paste0("There are not 25 columns of data in this file.\n Check the file before proceeding"))
    stop("There are not 25 columns of data in this file.\n Check the file before proceeding")
  }

  # Check to make sure column 1 is "Original Sample" or other?
  if (any(colnames(df.wq)[1] != "Original Sample" & df.wq[25] != "X Ldl")) {
    # Send warning message to UI
    #warning2 <- print(paste0("At least 1 column heading is unexpected.\n Check the file before proceeding"))
    stop("At least 1 column heading is unexpected.\n Check the file before proceeding")
  }

  #Dup check  - need to do this after UniqueID Column is created. See below...

# Any other checks?  Otherwise data is validated, proceed to reformatting...
###

### Need to insert code here to abort function if any warnings are triggered


# Connect to db for queries below
con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}",
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
                 "SampleDateTime",
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
# Remove time column - not needed
df.wq <- df.wq[,-9]

# Add in the timezone to the datetime format
df.wq$SampleDateTime <- as.POSIXct(paste(df.wq$SampleDateTime, format = "%Y-%m-%d %H:%M"), tz = "America/New_York", usetz = T)

# Fix other data types
df.wq$EDEP_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$EDEP_MW_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$Comment <- as.character(df.wq$Comment)

# Load tblParameters to access abbreviation for Unique ID
params <- dbReadTable(con,"tblParameters")
# df.wq$Parameter <- params$ParameterName[match(df.wq$Parameter, params$ParameterMWRAName)]

# Delete possible Sample Address rows (Associated with MISC Sample Locations):
df.wq <- filter(df.wq, !is.na(ResultReported)) # Filter out any sample with no results (There shouldn't be, but they do get included sometimes)

######################
#   Add new Columns  #
######################

### Unique ID number
df.wq$UniqueID <- ""
df.wq$UniqueID <- paste(df.wq$Location, format(df.wq$SampleDateTime, format = "%Y-%m-%d %H:%M"), params$ParameterAbbreviation[match(df.wq$Parameter, params$ParameterName)], sep = "_")

## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df.wq$UniqueID))
dupes <- df.wq$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste0("This data file contains ", length(dupes),
             " records that appear to be duplicates. Eliminate all duplicates before proceeding"))
  #print(dupes) # Show the duplicate Unique IDs to user in Shiny
}
### Make sure records are not already in DB

Uniq <- dbGetQuery(con, paste0("SELECT UniqueID, ID FROM ", ImportTable))
dupes2 <- Uniq$UniqueID[Uniq$UniqueID %in% df.wq$UniqueID]

if (length(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste0("This data file contains ", length(dupes2),
              " records that appear to already exist in the database! Eliminate all duplicates before proceeding"))
  #print(dupes2) # Show the duplicate Unique IDs to user in Shiny
}
rm(Uniq)

### DataSource
df.wq <- df.wq %>% mutate(DataSource = paste0("Turbidity_", month(max(df.wq$SampleDateTime)),"-",year(max(df.wq$SampleDateTime))))

### DataSourceID
# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(SampleDateTime, Location, Parameter)),]

# Assign the numbers
df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)

### FinalResult (numeric)
# Make the variable
df.wq$FinalResult <- NA
# Set the vector for mapply to operate on
x <- df.wq$ResultReported
# Function to determine FinalResult
  FR <- function(x) {
    if(str_detect(x, "<")) {# BDL
      # THEN strip "<" from reported result, make numeric, divide by 2.
      as.numeric(gsub("<","", x), digits =4)/2
    } else if(str_detect(x, ">")) { # ADL
      # THEN strip ">" form reported result, make numeric.
      as.numeric(gsub(">","", x))
      } else as.numeric(x) # ELSE THEN just use Result Reported for Result and make numeric
  }
  df.wq$FinalResult <- mapply(FR,x) %>%
    round(digits = 4)
### Flag (numeric)
  # Use similar function as to assign flags
  df.wq$FlagCode <- NA
   FLAG <- function(x) {
    if(str_detect(x, "<")) {# BDL
      # THEN set to 100
      100
    } else if(str_detect(x, ">")) { # ADL
      # THEN set to 101
      101
    } else NA # No flag
   }
  df.wq$FlagCode <- mapply(FLAG,x) %>% as.numeric()

### Storm SampleN (numeric)
df.wq$StormSampleN <- NA %>% as.character()

### Importdate (Date)
df.wq <- df.wq %>% mutate(ImportDate = today())

#####################################################################

### IDs

### IDs
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

##################
# Reformatting 2 #
##################

# Deselect Columns that do not need in Database
df.wq <- df.wq %>% select(-c(Description, FlagCode))

# Reorder remaining 32 columns to match the database table exactly
col.order.wq <- dbListFields(con, ImportTable)
df.wq <-  df.wq[,col.order.wq]

# Create a list of the processed datasets
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- path
dfs[[3]] <- df.flags
# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION

#### COMMENT OUT WHEN RUNNING SHINY ##############
#
#             #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
#             dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL)
#
#             # Extract each element needed
#             df.wq     <- dfs[[1]]
#             path  <- dfs[[2]]

##################################################

##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){

  con <-  odbcConnectAccess(filename.db)

  # Import the data to the database - Need to use RODBC methods here. Tried odbc and it failed
  # WQ Data
  ColumnsOfTable <- sqlColumns(con, ImportTable)
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME)
  sqlSave(con, df.wq, tablename = ImportTable, append = T,
          rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)
  # Flag data
  if (!is.na(df.flags)){ # Check and make sure there is flag data to import
    sqlSave(con, df.flags, tablename = ImportFlagTable, append = T,
            rownames = F, colnames = F, addPK = F , fast = F)
  }
  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)
}
#IMPORT_DATA(df.wq, df.flags, path, file, filename.db, processedfolder = NULL, ImportTable, ImportFlagTable = NULL)
### END
