##############################################################################################################################
#     TITLE: ImportMWRA_Q.R
#     DESCRIPTION: This script will Format/Process MWRA data to DCR
#                  This script will process and import MWRA Projects: QRTRIB, WRTRIB, MDHEML, QR-WQM
#     AUTHOR(S): Dan Crocker; modified by Brett Boisjolie
#     DATE LAST UPDATED: 2021-12-01
#     GIT REPO: WIT
#    
##############################################################################################################################

# NOTE - THIS SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

# # Load libraries needed
#library(tidyverse)
#library(stringr)
#library(odbc)
#library(DBI)
#library(lubridate)
#library(magrittr)
#library(readxl)
# library(testthat)
# library(glue)
# library(writexl)
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
  
  # Connect to db for queries below
  ### Connect to Database   
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- "Quabbin"
  tz <- 'America/New_York'
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)
  
  
  #################################
  #  START REFORMATTING THE DATA  #
  #################################
  
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
  
  ### Rename Columns in Raw Data
  names(df.wq) = c("SampleGroup",
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
                   "DetectionLimit")
  
  ### Perform Data checks ###
  
  # At this point there could be a number of checks to make sure data is valid
  
  # # Check to see if there were any miscellaneous locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Location, "QUABBIN-MISC"),TRUE)) > 0) {
    #Send warning message to UI if TRUE
    stop("There are unspecified (MISC) locations that need to be corrected before importing data")
  }
  # Check to see if there were any GENERAL locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Location, "GENERAL-GEN"),TRUE)) > 0) {
    # Send warning message to UI if TRUE
    stop("There are unspecified (GEN) locations that need to be corrected before importing data")
  }
  
  ##Remove Wachusett data
  df.wq <- df.wq %>% filter(!str_detect(Location,"^WACHUSET"))
  
  ##Separate and export MW data
  df.mw <- df.wq %>% filter(str_detect(Location,"^MW-"))
  exportpath <- "//env.govt.state.ma.us/enterprise/DCR-Quabbin-WKGRP/EQINSPEC/WQDatabase/data/wqDistributionSystemResults/staging/"
  fullexportpath <- gsub(" ","",paste (exportpath,"MW_",file))
  write_xlsx(df.mw, fullexportpath) 
  df.wq <- df.wq %>% filter(!str_detect(Location,"^MW-"))
  
  # Any other checks?  Otherwise data is validated, proceed to reformatting...
  ###
  
  ### Date and Time:
  # SampleDateTime
  # Split the Sample time into date and time
  df.wq$SampleDate <- as.Date(df.wq$SampleDate)
  #df.wq$SampleTime[is.na(df.wq$SampleTime)] <- paste(df.wq$SampleDate[is.na(df.wq$SampleTime)])
  
  df.wq <- separate(df.wq, SampleTime, into = c("date", "Time"), sep = " ")
  
  # Merge the actual date column with the new Time Column and reformat to POSIXct
  df.wq$DateTimeET <- as.POSIXct(paste(as.Date(df.wq$SampleDate, format ="%Y-%m-%d"), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = T)
  
  #Import Staff
  ImportStaff <- "DCR.Quabbin"
  
  # Fix other data types
  df.wq$EDEP_Confirm <- as.character(df.wq$EDEP_Confirm)
  df.wq$EDEP_MW_Confirm <- as.character(df.wq$EDEP_Confirm)
  df.wq$Comment <- as.character(df.wq$Comment)
  df.wq$ResultReported <- as.character(df.wq$ResultReported)
  df.wq$SampleGroup <- as.character(df.wq$SampleGroup)
  
  if(all(!is.na(df.wq$LabRecDateET))) {
    df.wq$LabRecDateET <- as.POSIXct(paste(df.wq$LabRecDateET, format = "%Y-%m-%d %H:%M:SS", tz = "America/New_York", usetz = T))
  } else {
    df.wq$LabRecDateET <- as_datetime(df.wq$LabRecDateET)
  }
  
  if(all(!is.na(df.wq$PrepOnET))) {
    df.wq$PrepOnET <- as.POSIXct(paste(df.wq$PrepOnET, format = "%Y-%m-%d %H:%M:SS", tz = "America/New_York", usetz = T))
  } else {
    df.wq$PrepOnET <- as_datetime(df.wq$PrepOnET)
  }
  
  if(all(!is.na(df.wq$DateTimeAnalyzedET))) {
    df.wq$DateTimeAnalyzedET <- as.POSIXct(paste(df.wq$DateTimeAnalyzedET, format = "%Y-%m-%d %H:%M:SS", tz = "America/New_York", usetz = T))
  } else {
    df.wq$DateTimeAnalyzedET <- as_datetime(df.wq$DateTimeAnalyzedET)
  }
  
  ### Fix the Parameter names ####  - change from MWRA name to ParameterName
  params <- dbReadTable(con,  Id(schema = "Wachusett", table = "tblParameters"))
  df.wq$Parameter <- params$ParameterName[match(df.wq$Parameter, params$ParameterMWRAName)]
  
  
  ### Remove records with missing elements/unneeded data ####
  # Delete possible Sample Address rows (Associated with MISC Sample Locations):
  df.wq <- df.wq %>%  # Filter out any sample with no results (There shouldn't be, but they do get included sometimes)
    filter(!is.na(Parameter),
           !is.na(ResultReported))
  
  df.wq <- df.wq %>% slice(which(!grepl("Sample Address", df.wq$Parameter, fixed = TRUE)))
  df.wq <- df.wq %>% slice(which(!grepl("(DEP)", df.wq$Parameter, fixed = TRUE))) # Filter out rows where Parameter contains  "(DEP)"
  df.wq <- df.wq %>% slice(which(!grepl("X", df.wq$Status, fixed = TRUE))) # Filter out records where Status is X
  
  # Fix the Location names
  df.wq$Location %<>%
    gsub("QUABBINT-","", .) %>%
    gsub("QUABBIN-","", .)
  
  ######################
  #   Add new Columns  #
  ######################
  
  ### Unique ID number
  df.wq$UniqueID <- NA_character_
  df.wq$UniqueID <- paste(df.wq$Location, format(df.wq$DateTimeET, format = "%Y-%m-%d %H:%M"), params$ParameterAbbreviation[match(df.wq$Parameter, params$ParameterName)], df.wq$Analysis, sep = "_")
  
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
  
  
  ### Make sure records are not already in DB
  
  Uniq <- dbGetQuery(con, glue("SELECT [UniqueID], [ID] FROM [{schema}].[{ImportTable}]"))
  flags <- dbGetQuery(con, glue("SELECT [SampleID], [FlagCode] FROM [{schema}].[{ImportFlagTable}] WHERE FlagCode = 102"))
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
  df.wq <- df.wq %>% 
    mutate(DataSource = paste0("MWRA_", file))
  
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
  
  # Add new column for censored data
  df.wq <- df.wq %>%
    mutate("IsCensored" = NA_integer_)
  
  df.wq$IsCensored <- as.logical(df.wq$IsCensored)
  if(length(edits) == 0) {
    df.wq$IsCensored <- FALSE
  } else {
    df.wq$IsCensored[edits] <- TRUE
    df.wq$IsCensored[-edits] <- FALSE
  }
  
  
  ### FinalResult (numeric)
  # Make the variable
  df.wq$FinalResult <- NA
  
  # Set the vector for mapply to operate on
  x <- df.wq$ResultReported
  
  # Function to determine FinalResult
  FR <- function(x) {
    if(str_detect(x, "<")){# BDL
      as.numeric(gsub("<","", x), digits =4)  # THEN strip "<" from reported result, make numeric
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
  df.wq$ImportDate <- Sys.Date() %>% force_tz("America/New_York")
  
  ######################################
  # REMOVE ANY PRELIMINARY DATA     ###
  ######################################
  
  # Calculate the date range of import
  datemin <- min(df.wq$DateTimeET)
  datemax <- max(df.wq$DateTimeET)
  
  # IDs to look for - all records in time period in question
  qry <- glue("SELECT (ID) FROM [{schema}].[{ImportTable}] WHERE [DateTimeET] >= '{datemin}' AND [DateTimeET] <= '{datemax}'")
  query.prelim <- dbGetQuery(con, qry) # This generates a list of possible IDs
  
  if (nrow(query.prelim) > 0) {# If true there is at least one record in the time range of the data
    # SQL query that finds matching sample ID from tblSampleFlagIndex Flagged 102 within date range in question
    qryS <- glue("SELECT [SampleID] FROM [{schema}].[{ImportFlagTable}] WHERE [FlagCode] = 102 AND [SampleID] IN ({paste0(query.prelim$ID, collapse = ",")})")
    qryDelete <- dbGetQuery(con,qryS) # Check the query to see if it returns any matches
    # If there are matching records then delete preliminary data (IDs flagged 102 in period of question)
    if(nrow(qryDelete) > 0) {
      qryDeletePrelimData <- glue("DELETE FROM [{schema}].[{ImportTable}] WHERE [DataTableName] = 'tblMWRAResults' AND [ID] IN ({paste0(qryDelete$SampleID, collapse = ",")})")
      rs <- dbSendStatement(con,qryDeletePrelimData)
      print(paste(dbGetRowsAffected(rs), "preliminary records were deleted during this import", sep = " ")) # Need to display this message to the Shiny UI
      dbClearResult(rs)
      
      # Next delete all flags associated with preliminary data - Will also delete any other flag associated with record number
      qryDeletePrelimFlags <- glue("DELETE FROM [{schema}].[{ImportFlagTable}] WHERE [DataTableName] = 'tblMWRAResults' AND [SampleID] IN ({paste0(qryDelete$SampleID, collapse = ",")})")
      rs <- dbSendStatement(con, qryDeletePrelimFlags)
      print(paste(dbGetRowsAffected(rs), "preliminary record data flags were deleted during this import", sep = " "))
      dbClearResult(rs)
    }
  }
  
  
  ########################################################################.
  ###                               SET IDs                           ####
  ########################################################################.
  
  # Read Tables
  # WQ ##
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
  
  ### Flags ####
  
  # First make sure there are flags in the dataset
  setFlagIDs <- function(){
    if(all(is.na(df.wq$FlagCode)) == FALSE){ # Condition returns FALSE if there is at least 1 non-NA value, if so proceed
      # Split the flags into a separate df and assign new ID
      df.flags <- as.data.frame(select(df.wq, c("ID","FlagCode"))) %>%
        rename("SampleID" = ID) %>%
        drop_na()
    } else {
      df.flags <- NA
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
      
      ### ID flags ###
      df.flags$ID <- seq.int(nrow(df.flags)) + ID.max.flags
      df.flags$DataTableName <- ImportTable
      df.flags$DateFlagged <-  Sys.Date() %>% force_tz("America/New_York")
      df.flags$ImportStaff <-  ImportStaff
      df.flags$Comment <- "Flag automatically added at import"
      
      # Reorder df.flags columns to match the database table exactly # Add code to Skip if no df.flags
      
      df.flags <- df.flags[,c(3,4,1,2,5,6,7)]
    } else { # Condition TRUE - All FlagCodes are NA, thus no df.flags needed, assign NA
      df.flags <- NA
    } # End flags processing chunk
  } # End set flags function
  df.flags <- setFlagIDs()
  
  
  ##############################################################################################################################
  # Reformatting 2
  ##############################################################################################################################
  
  ### Deselect Columns that do not need to be in Database
  df.wq <- df.wq %>% select(-c(Description,
                               SampleDate,
                               FlagCode,
                               date,
                               Time
  )
  )
  
  # Reorder remaining 30 columns to match the database table exactly ####
  col.order.wq <- dbListFields(con, schema_name = schema, name = ImportTable)
  df.wq <-  df.wq[,col.order.wq]
  
  #THIS IS SUPER COOL - REVISE FOR QUABBIN LATER  ## QC Test ####
  #source(paste0(getwd(),"/src/Functions/WITQCTEST.R"))
  #qc_message <- QCCHECK( df.qccheck = df.wq, 
  #                       file = file, 
  #                       ImportTable = ImportTable)
  #print(qc_message)
  
  # Create a list of the processed datasets
  dfs <- list()
  dfs[[1]] <- df.wq
  dfs[[2]] <- path
  dfs[[3]] <- df.flags # Removed condition to test for flags and put it in the setFlagIDS() function
  
  # Disconnect from db and remove connection obj
  dbDisconnect(con)
  rm(con)
  return(dfs)
} # END FUNCTION ####

#### COMMENT OUT SECTION BELOW WHEN RUNNING SHINY
########################################################################################################
# #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
#dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = ImportFlagTable)
#
# # Extract each element needed
#df.wq     <- dfs[[1]]
#path      <- dfs[[2]]
#df.flags  <- dfs[[3]]

##########################
# Write data to Database #
##########################
# processed_subdir <- paste0("/", max(year(df.wq$DateTimeET))) # Raw data archived by year, subfolders = Year
# processed_dir <- paste0(processedfolder, processed_subdir)
# if(!file.exists(processed_dir)) {
#   dir.create(processed_dir)
# }


IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
  # df.flags is an optional argument  - not used for this dataset
  
  # Establish db connection
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- 'Quabbin'
  tz <- 'America/New_York'
  
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)  # Get Import Table Columns
  ColumnsOfTable <- dbListFields(con, schema = schema, ImportTable)
  
  # # Set variable types
  # varTypes  <- as.character(ColumnsOfTable$TYPE_NAME)
  # sqlSave(con, df.wq, tablename = ImportTable, append = T,
  #         rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)
  
  odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportTable}")), value = df.wq, append = TRUE)
  
  # Disconnect from db and remove connection obj
  dbDisconnect(con)
  rm(con)
  
  ### Move the processed raw data file to the processed folder ####
  processed_subdir <- paste0("/", max(year(df.wq$DateTimeET))) # Raw data archived by year, subfolders = Year
  processed_dir <- paste0(processedfolder, processed_subdir)
  if(!file.exists(processed_dir)) {
    dir.create(processed_dir)
  }
  
  file.rename(path, paste0(processed_dir,"/", file))
  
  return("Import Successful")
}

### END

