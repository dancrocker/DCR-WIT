##############################################################################################################################
#     Title: ImportFieldParameters.R
#     Description: This script will Format/Process/Import YSI, turbidity, and stage data
#     Written by: Dan Crocker & Travis Drury
#     Last Update: November 2019
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
data <- openxlsx::read.xlsx(wbobj, sheet =  1, startRow = 1, colNames = T, cols = c(2:14))

if (nrow(data)==0){
  # Send warning message to UI
  stop("There are no records in the file! Make sure there are records and try again.")
}

expectedcolumns<-c("Source.Name","Timestamp","Specific.Conductance.(uS/cm)","Dissolved.Oxygen.(mg/L)","pH_1.(Units)","Temperature.(C)","Comment","Site","Folder","Unit.ID","Turbidity.(NTU)","Stage.(feet)","Sampled.By")

if (all(colnames(data)!=expectedcolumns)){
  # Send warning message to UI
  stop("There are unexpected column names in the file! Check Field Parameter Data Importer and try again.")
}


# Get the data in order and formatted
    df <- gather(data,Parameter,FinalResult,3:6,11,12)
    df$Timestamp <- XLDateToPOSIXct(df$Timestamp)
    df <- separate(df, Timestamp, into = c("Date", "Time"), sep = " ")
    df$SampleDateTime <- as.POSIXct(paste(as.Date(df$Date, format ="%Y-%m-%d"), df$Time, sep = " "), format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York", usetz = T)
    df <- df[,-c(2,3,6,7)]
    df <- dplyr::rename(df, "Location" = Site, "DataSource" = Source.Name, "SampledBy" = Sampled.By)

    df$FinalResult <- as.numeric(df$FinalResult)
    df$Location <- as.factor(df$Location)
    df$SampledBy <- as.factor(df$SampledBy)
    df$Parameter <- as.factor(df$Parameter)
    
    

# Rename parameters factors
    levels(df$Parameter)[levels(df$Parameter)=="Dissolved.Oxygen.(mg/L)"] <-"Dissolved Oxygen"
    levels(df$Parameter)[levels(df$Parameter)=="pH_1.(Units)"] <-"pH"
    levels(df$Parameter)[levels(df$Parameter)=="Specific.Conductance.(uS/cm)"] <-"Specific Conductance"
    levels(df$Parameter)[levels(df$Parameter)=="Stage.(feet)"] <-"Staff Gauge Height"
    levels(df$Parameter)[levels(df$Parameter)=="Temperature.(C)"] <-"Water Temperature"
    levels(df$Parameter)[levels(df$Parameter)=="Turbidity.(NTU)"] <-"Turbidity NTU"
    
    
# Create column for units
    df$Units <- NA
    df$Units <- ifelse(df$Parameter=="Dissolved Oxygen","mg/L",
                ifelse(df$Parameter=="pH","pH",
                ifelse(df$Parameter=="Specific Conductance","uS/cm",
                ifelse(df$Parameter=="Staff Gauge Height","ft",
                ifelse(df$Parameter=="Water Temperature","Deg-C",
                ifelse(df$Parameter=="Turbidity NTU","NTU",NA
                       ))))))
    
# Round times
    

    # Connect to db for query below
    filename.db = filename.db()
    
    con <- dbConnect(odbc::odbc(),
                     .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}",
                                                paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                     timezone = "America/New_York")
    locations <- dbReadTable(con,"tblLocations")
    flowlocations <- filter(locations, !is.na(LocationFlow))
    
    df$SampleDateTime <- round_date(df$SampleDateTime,"minute")
    
    df$SampleDateTime <- ifelse(df$Location=="MD04",
                                round_date(df$SampleDateTime,"10 minutes"),
                                ifelse(df$Location %in% flowlocations$LocationMWRA,
                                round_date(df$SampleDateTime,"15 minutes"),
                                df$SampleDateTime))
    
    df$SampleDateTime <- as.POSIXct(df$SampleDateTime, origin="1970-01-01 00:00:00", format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = F)
    
    dbDisconnect(con)
    rm(con)
    
# Remove rows with no data value (can happen if one parameter is unable to sampled for example)   
    df <- filter(df, !is.na(FinalResult))
    
# Find the row to paste the data on sheet 2, then copy the data over
    PasteRow <- as.numeric(NROW(read.xlsx(wb, sheet =  "ImportedToWQDB", colNames = F, cols = 1)) + 1)
    openxlsx::writeData(wbobj, sheet = 2, data , startCol = 1, startRow = PasteRow, colNames = F)
    
# Find the last row of data to delete on sheet 1, delete the data, then save the workbook
    EndRow <- NROW(data) + 2
    openxlsx::deleteData(wbobj, sheet = 1, cols = 2:14, rows = 2:EndRow, gridExpand = T)
    openxlsx::saveWorkbook(wbobj, wb, overwrite = TRUE)
    
# Copy the columns into the import template spreadsheet:
    # Open the Workbook and create a workbook object to manipulate
    wb <- config[24]
    wbobj <- loadWorkbook(wb)
    # Find the last row in the import table
    EndRow <- as.numeric(NROW(read.xlsx(wbobj, sheet = 1, colNames = T)) + 1)
    # Delete the old data
    deleteData(wbobj, sheet = 1, cols = 1:25, rows = 2:EndRow, gridExpand = T)
    # Insert the new data
    writeData(wbobj, sheet = 1, df[,3] , startCol = 4, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,6] , startCol = 16, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,4] , startCol = 19, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,5] , startCol = 15, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,8] , startCol = 17, startRow = 2, colNames = F)
    writeData(wbobj, sheet = 1, df[,7] , startCol = 8, startRow = 2, colNames = F)
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
importerpath <- config[24]
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

# Fix the Location names
df.wq$Location %<>%
  gsub("WACHUSET-","", .) %>%
  gsub("M754","MD75.4", .) %>% 
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
ToCalc <- filter(df.wq, Location %in% ratings$MWRA_Loc[ratings$Current == TRUE], Parameter == "Staff Gauge Height")
if(nrow(ToCalc) > 0){ # If TRUE then there are discharges to be calculated
  # call function in separate script to create df of discharges and df of flags to bind to main dfs
  source(paste0(getwd(),"/src/Functions/calcDischarges.R"))
  Q_dfs <- calcQ(filename.db = filename.db, stages = ToCalc)
  # Extract the 2 dfs out of the list
  df_Q <- Q_dfs$df_Q
  df_QFlags <- Q_dfs$df_QFlags
  df.wq$ResultReported <- as.character(df.wq$ResultReported)
  df.wq <- bind_rows(df.wq,df_Q)
  # Merge in Discharge Records
} else {
  print("No stage records available for discharge calculations")
}

###################################################


## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df.wq$UniqueID))
dupes <- df.wq$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste0("This data file contains ", length(dupes),
             " records that appear to be duplicates. Eliminate all duplicates before proceeding"))
  print(dupes) # Show the duplicate Unique IDs to user in Shiny
}
### Make sure records are not already in DB


Uniq <- dbGetQuery(con, paste0("SELECT UniqueID, ID FROM ", ImportTable))
dupes2 <- Uniq$UniqueID[Uniq$UniqueID %in% df.wq$UniqueID]

if (length(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste0("This data file contains ", length(dupes2),
              " records that appear to already exist in the database! Eliminate all duplicates before proceeding"))
  print(dupes2) # Show the duplicate Unique IDs to user in Shiny
}
rm(Uniq)

### DataSource
df.wq <- df.wq %>% mutate(DataSource = paste0("Field_Parameters_", min(as.Date(df.wq$SampleDateTime, format= "%y-%m-%d")),"_",max(as.Date(df.wq$SampleDateTime, format= "%y-%m-%d"))))

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
  if(all(is.na(df.wq$FlagCode)) == FALSE){ # Condition returns FALSE if there is at least 1 non-NA value, if so proceed
    # Split the flags into a separate df and assign new ID
    df.flags <- as.data.frame(select(df.wq,c("ID","FlagCode"))) %>%
    rename("SampleID" = ID) %>%
      drop_na()
    fc <- 1
  } else {
    df.flags <- NA
    fc <- 0
  }
  # Get discharge flags (if any)
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
    query.flags <- dbGetQuery(con, paste0("SELECT max(ID) FROM ", ImportFlagTable))
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
    flag.col.order.wq <- dbListFields(con, ImportFlagTable)
    df.flags <-  df.flags[,flag.col.order.wq]
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

# QC Test
source(paste0(getwd(),"/src/Functions/WITQCTEST.R"))
qc_message <- QCCHECK(df.qccheck=df.wq,file=file,ImportTable=ImportTable)
print(qc_message)

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
            # #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
            # dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL)
            # 
            # # Extract each element needed
            # df.wq     <- dfs[[1]]
            # path  <- dfs[[2]]

##################################################

##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.wq, df.flags=NULL , path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){

  con <-  odbcConnectAccess(filename.db)

  # Import the data to the database - Need to use RODBC methods here. Tried odbc and it failed
  # WQ Data
  ColumnsOfTable <- sqlColumns(con, ImportTable)
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME)
  sqlSave(con, df.wq, tablename = ImportTable, append = T,
          rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)

  #Move Preliminary csv files to the processed data folder
  rawYSI <- config[25]
  filelist <- list.files(rawYSI,".csv$")
  filelist2 <- paste0(rawYSI,"/", filelist)
  file.rename(filelist2, paste0(config[26],"/", filelist))
  
    # Flag data
  if (class(df.flags) == "data.frame"){ # Check and make sure there is flag data to import 
   df.flags$DateFlagged <- as.Date(df.flags$DateFlagged, format ="%Y-%m-%d")
      sqlSave(con, df.flags, tablename = ImportFlagTable, append = T,
            rownames = F, colnames = F, addPK = F , fast = F)
    } else {
      print("There were no flags to import")
    }

  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)

  return("Import Successful")  
  
 }


#IMPORT_DATA(df.wq, df.flags, path, file, filename.db, processedfolder = NULL, ImportTable, ImportFlagTable = NULL)
### END
 