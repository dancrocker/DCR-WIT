##############################################################################################################################
#     Title: ImportPrelimBacteria.R
#     Description: This script will Format/Process Preliminary MWRA data to WQ Database
#     Written by: Dan Crocker, December, 2017
#
#    This script will process and import MWRA Projects: WATTRB, WATTRN Preliminary data as csv files
#
##############################################################################################################################

# NOTE - THIS TOP SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT SECTION BELOW WHEN RUNNING FUNCTION IN SHINY

            # Load libraries needed

#             library(tidyverse)
#             library(stringr)
#             library(odbc)
#             library(RODBC)
#             library(DBI)
#             library(readxl)
#             library(lubridate)
#             library(magrittr)
#             library(openxlsx)
#             library(data.table)


##############
# PREP DATA  #  This function combines all of the preliminary bacteria csv files into 1 xlsx file
##############

PREP_DATA <- function(rawdatafolder){

#These variables are the same as what is listed in datasets.xlsx
rawdatafolder <- rawdatafolder

### Generate a list of the preliminary data files:
filelist <- grep(
  x = list.files(rawdatafolder, ignore.case = T, include.dirs = F),
  pattern = "^DCRBACT_[0-9]*.csv$", # regex to show xlsx files, but filter out lockfiles string = "$"
  value = T,
  perl =T)

# Add the path to each file and save as a new list
filelist2 <- paste0(rawdatafolder,"/", filelist) # This will print the list of contents in the folder
###################################################################################################
# Set system environments (Future - try to set this up to be permanent)
# Without setting these envs the openxlsx saveWorkbook fn cannot zip the file and save it
          Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe")
          Sys.setenv(PATH = paste("C:/Rtools/bin", Sys.getenv("PATH"), sep=";"))
          Sys.setenv(BINPREF = "C:/Rtools/mingw_$(WIN)/bin/")
          # Check system environments
          # Sys.getenv("R_ZIPCMD", "zip")
          # Sys.getenv("PATH") # Rtools should be listed now
###################################################################################################
# Read in all preliminary files and combine into 1
tables <- lapply(filelist2, read.csv, header = TRUE)
#a <-  tables[[3]]

# Combine files
combined.df <- rbindlist(tables)
# mutate_at(vars(RESULT_ENTRY),funs('as.factor'))

# Filter out unneeded columns and save to new df
df.wq <- combined.df[, -c(16,18:22,24:25)]
# Rename Columns to match existing format
names(df.wq) = c("SampleGroup",
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
                 "Parameter",
                 "ResultReported",
                 "Units",
                 "Comment",
                 "SampledBy",
                 "Status")

# Add missing variables:

                 df.wq$TextID <-  ""
                 df.wq$ReportedName <-  ""
                 df.wq$SampleNumber <-  ""
                 df.wq$EDEP_Confirm <- as.character(df.wq$EDEP_Confirm)
                 df.wq$EDEP_MW_Confirm <- as.character(df.wq$EDEP_Confirm)
                 df.wq$Reportable <-  ""
                 df.wq$Method <-  ""
                 df.wq$DetectionLimit <-  ""
                 df.wq$Comment <- as.character(df.wq$Comment)
                 df.wq$ResultReported <- as.character(df.wq$ResultReported)

# Create a workbook object and add df.wq to it - save over the older workbook

      wb <- paste0(rawdatafolder,"/PrelimBacteria.xlsx")
      wbobj <- createWorkbook(wb)
      addWorksheet(wbobj, "Sheet 1")
      writeData(wbobj, sheet = 1, df.wq, colNames = T)
# Save the workbook and then proceed (This will overwrite the existing file)
saveWorkbook(wbobj, wb, overwrite = TRUE)
} # End PREP DATA FUNCTION

#############################
#   PROCESSING FUNCTION    #
############################
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable){
options(scipen = 999) # Eliminate Scientific notation in numerical fields
# Get the full path to the file

PREP_DATA(rawdatafolder = rawdatafolder)

  path <- paste0(rawdatafolder,"/", file)
  # file <- "PrelimBacteria.xlsx"
# Read in the data to a dataframe
df.wq <- read_excel(path, sheet= 1, col_names = T, trim_ws = T, na = "nil") %>%
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
  if (any(colnames(df.wq)[1] != "SampleGroup" & df.wq[17] != "Status")) {
    # Send warning message to UI
    #warning2 <- print(paste0("At least 1 column heading is unexpected.\n Check the file before proceeding"))
    stop("At least 1 column heading is unexpected.\n Check the file before proceeding")
  }

  # Check to see if there were any miscellaneous locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Name, "MISC"),TRUE)) > 0) {
    #warning3 <- print(paste0("There are unspecified (MISC) locations that need to be corrected before importing data"))
    stop("There are unspecified (MISC) locations that need to be corrected before importing data")
  }


### OTHER MESSAGES AND WARNINGS:
# "All MISC sample locations were automatically converted to location MD75.4
#-- If other miscellaneous Locations were part of this datset they should be corrected prior to importing data to database

# Connect to db for queries below
### Connect to Database   
dsn <- filename.db
database <- "DCR_DWSP"
schema <- "Wachusett"
tz <- 'America/New_York'
con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)

#################################
#  START REFORMATTING THE DATA  #
#################################

# Merge the actual date column with the new Time Column and reformat to POSIXct
df.wq$DateTimeET <- as.POSIXct(paste(as.Date(df.wq$SampleDate, format ="%m/%d/%Y"), df.wq$SampleTime, sep = " "), format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = T)

# Fix other data types
df.wq$EDEP_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$EDEP_MW_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$Comment <- as.character(df.wq$Comment)

# Fix the Parameter names  - change from MWRA name to ParameterName
params <- dbReadTable(con,  Id(schema = schema, table = "tblParameters"))
df.wq$Parameter <- params$ParameterName[match(df.wq$Parameter, params$ParameterMWRAName)]

# Delete possible Sample Address rows (Associated with MISC Sample Locations):
df.wq <- filter(df.wq, !is.na(ResultReported)) # Filter out any sample with no results (There shouldn't be, but they do get included sometimes)

df.wq <- df.wq %>% slice(which(!grepl("Sample Address", df.wq$Parameter, fixed = TRUE)))

# Fix the Location names
df.wq$Location %<>%
  gsub("WACHUSET-","", .) %>% 
  gsub("M754","MD75.4", .)

######################
#   Add new Columns  #
######################

### Unique ID number ####
df.wq$UniqueID <- NA_character_
df.wq$UniqueID <- paste(df.wq$Location, format(df.wq$DateTimeET, format = "%Y-%m-%d %H:%M"), params$ParameterAbbreviation[match(df.wq$Parameter, params$ParameterName)], sep = "_")

## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df.wq$UniqueID))
dupes <- df.wq$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste0("This data file contains ", length(dupes),
             " records that appear to be duplicates. Eliminate all duplicates before proceeding",
             "The duplicate records include: ", paste(head(dupes, 15), collapse = ", ")), call. = FALSE)
}
### Make sure records are not already in DB

Uniq <- dbGetQuery(con, glue("SELECT [UniqueID], [ID] FROM [{schema}].[{ImportTable}]"))
dupes2 <- Uniq$UniqueID[Uniq$UniqueID %in% df.wq$UniqueID]

if (length(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste0("This data file contains ", length(dupes2),
              " records that appear to already exist in the database! Eliminate all duplicates before proceeding",
              "The duplicate records include: ", paste(head(dupes2, 15), collapse = ", ")), call. = FALSE)
}
rm(Uniq)

### DataSource
df.wq <- df.wq %>% mutate(DataSource = paste("MWRA", file,today(), sep = "_"),
                          Imported_By = username,
                          QAQC_By = NA_character_)
### DataSourceID
# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(DateTimeET, Location, Parameter)),]

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
    as.numeric(gsub("<","", x), digits = 4) * 0.5 # THEN strip "<" from reported result, make numeric, divide by 2.
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
    100     # THEN set to 100 for BDL
  } else if (str_detect(x, ">")){
    101     # THEN set to 101 for ADL
  } else {
    NA
  }
}
df.wq$FlagCode <- mapply(FLAG,x) %>% as.numeric()

  ### Storm Sample (Actually logical data - yes/no, but Access stores as -1 for yes and 0 for no. 1 bit
  # df.wq$StormSample <- 0 %>%  as.numeric()

### Storm SampleN (numeric)
df.wq$StormSampleN <- NA %>% as.character()

### Importdate (Date)
df.wq <- df.wq %>% mutate(ImportDate = today())

#####################################################################

### IDs
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

# First make sure there are flags in the dataset

setFlagIDs <- function(){

  query.flags <- dbGetQuery(con, glue("SELECT max(ID) FROM [{schema}].[{ImportFlagTable}]"))
  # Get current max ID
  if(is.na(query.flags)) {
    query.flags <- 0
  } else {
    query.flags <- query.flags
  }
  ID.max.flags <- as.numeric(unlist(query.flags))
  rm(query.flags)

  # Generate flags for above/below detection records
  df.flag1 <- as.data.frame(select(df.wq,c("ID","FlagCode"))) %>%
    rename("SampleFlag_ID" = ID) %>%
    drop_na()
  # Generate flags for every record indicating that it is a preliminary record = 102
  df.flag2 <- as.data.frame(select(df.wq,c("ID","FlagCode"))) %>%
    rename("SampleFlag_ID" = ID)
  df.flag2$FlagCode <- 102
  # Merge the two flag dfs into 1
  df.flags <- rbind(df.flag1, df.flag2)

  ### ID flags
  df.flags$ID <- seq.int(nrow(df.flags)) + ID.max.flags
  df.flags$DataTableName <- ImportTable
  df.flags$DateFlagged = today()
  df.flags$ImportStaff = Sys.getenv("USERNAME")

  # Reorder df.flags columns to match the database table exactly # Add code to Skip if no df.flags
  df.flags <- df.flags[,c(3,4,1,2,5,6)]
} # End set flags function
df.flags <- setFlagIDs()


##############################################################################################################################
# Reformatting 2
##############################################################################################################################

### Deselect Columns that do not need in Database
df.wq <- df.wq %>% select(-c(Description,
                             SampleDate,
                             FlagCode,
                             SampleTime
                             )
)

# Reorder remaining 32 columns to match the database table exactly
col.order.wq <- dbListFields(con, schema_name = schema, name = ImportTable)
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

#### COMMENT OUT WHEN RUNNING SHINY
########################################################################################################
#RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
#dfs <- PROCESS_DATA(file, rawdatafolder, filename.db)

## Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]

########################################################################################################

##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){

  start <- now()
  print(glue("Starting data import at {start}"))
  ### CONNECT TO DATABASE ####
  ### Set DB
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- 'Wachusett'
  tz <- 'America/New_York'
  ### Connect to Database 
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)
  
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

  # Move Preliminary csv files to the processed data folder
  rawdatafolder <- str_sub(path, 1, nchar(path) - 20)
  filelist <- grep(
    x = list.files(rawdatafolder, ignore.case = T, include.dirs = F),
    pattern = "^DCRBACT_[0-9]*.csv$", # regex to show xlsx files, but filter out lockfiles string = "$"
    value = T,
    perl =T)

  filelist2 <- paste0(rawdatafolder,"/", filelist)
  ### Create the destination directory if it does not yet exist ####
  sapply(paste0(processedfolder,"/", str_sub(filelist,9,12),"/PreliminaryBacteria/"), dir.create)
  file.rename(filelist2, paste0(processedfolder,"/", str_sub(filelist,9,12),"/PreliminaryBacteria/", filelist))
  end <- now()
  return(print(glue("Import finished at {end}, \n elapsed time {round(end - start)} seconds")))  
}
### END
#IMPORT_DATA(df.wq, df.flags, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable)

