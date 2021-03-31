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
# library(RODBC)
# library(DBI)
# library(magrittr)
# library(openxlsx)
# library(DescTools)
# library(lubridate)
# library(devtools)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

##############.
# PREP DATA  #  # Function to Prep Data for use with Import Function
##############.

PREP_DATA <- function(file){

  ########################################################.
  # Set system environments (Future - try to set this up to be permanent)
  Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe")
  Sys.setenv(PATH = paste("C:/Rtools/bin", Sys.getenv("PATH"), sep=";"))
  Sys.setenv(BINPREF = "C:/Rtools/mingw_$(WIN)/bin/")
  # Check system environments
  # Sys.getenv("R_ZIPCMD", "zip")
  # Sys.getenv("PATH") # Rtools should be listed now
  ########################################################.

# Extract the full data from the sheet
data <- readxl::read_excel(file, sheet = "YSI_Import") %>% 
  select(-1)

if (nrow(data)==0){
  # Send warning message to UI
  stop("There are no records in the file! Make sure there are records and try again.")
}

expectedcolumns<-c("Source.Name","Timestamp","Specific.Conductance.(uS/cm)","Dissolved.Oxygen.(mg/L)","pH_1.(Units)","Temperature.(C)","Comment","Site","Folder","Unit.ID","Turbidity.(NTU)","Stage.(feet)","Sampled.By")

if (all(colnames(data)!=expectedcolumns)){
  # Send warning message to UI
  stop("There are unexpected column names in the file! Check Field Parameter Data Importer and try again.")
}


# Get the data in order and formatted ####
df <- gather(data, Parameter, FinalResult, c(3:6,11,12), na.rm = T) %>% 
  select(-c(Folder, `Unit ID`)) %>% 
  dplyr::rename("Location" = Site, "DataSource" = Source.Name, "SampledBy" = `Sampled By`, "DateTimeET" = Timestamp) %>% 
  mutate("Units" = NA_character_)

df$Parameter <-  dplyr::recode(df$Parameter, "Dissolved Oxygen (mg/L)" = "Dissolved Oxygen",
                               "pH_1 (Units)" = "pH",
                               "Specific Conductance (uS/cm)" = "Specific Conductance",
                               "Stage (feet)" = "Staff Gauge Height",
                               "Temperature (C)" = "Water Temperature",
                               "Turbidity (NTU)" ="Turbidity NTU")

# Create column for units ####
    
df$Units <- ifelse(df$Parameter =="Dissolved Oxygen","mg/L",
                   ifelse(df$Parameter =="pH","pH",
                          ifelse(df$Parameter =="Specific Conductance","uS/cm",
                                 ifelse(df$Parameter =="Staff Gauge Height","ft",
                                        ifelse(df$Parameter == "Water Temperature","Deg-C",
                                               ifelse(df$Parameter == "Turbidity NTU","NTU", NA
                                               ))))))
# Round times ####

    ### Connect to Database   
    dsn <- "DCR_DWSP_App_R"
    database <- "DCR_DWSP"
    schema <- 'Wachusett'
    tz <- 'America/New_York'
    con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)

    locations <- dbReadTable(con,  Id(schema = schema, table = "tblWatershedLocations"))
    flowlocations <- filter(locations, !is.na(LocationFlow))
    
    df$DateTimeET <- round_date(df$DateTimeET, "minute") 
    
    df$DateTimeET <- ifelse(df$Location == "MD04",
                                round_date(df$DateTimeET, "10 minutes"),
                                ifelse(df$Location %in% flowlocations$LocationMWRA,
                                round_date(df$DateTimeET, "15 minutes"),
                                df$DateTimeET))
    
    df$DateTimeET <- as_datetime(df$DateTimeET) %>% force_tz("America/New_York") 
    dbDisconnect(con)
    rm(con)
    
    ### Fix location names
    df$Location %<>%
      gsub("M754","MD75.4",.)

    if (any(!unique(df$Location) %in% locations$LocationMWRA)) {
      stop("There is at least one location in this data that is not yet recorded in the database. Either remove the record or rename the location to MISC and add result to MISC sample table after import.")
    }
# Remove rows with no data value (can happen if one parameter is unable to sampled for example)   
    # df <- filter(df, !is.na(FinalResult)) ### rm.na added to gather function above, which eliminates empty records
    
  print ("Data prep complete...")
  return(df)  
} # End PREP DATA Function
# df <- PREP_DATA(file = paste0(rawdatafolder, "/",file))

UPDATE_IMPORTER_XL <- function() {
  print("Updating Field Parameter Importer excel file ...")
  # Copy the columns into the import template spreadsheet:
  # Open the Workbook and create a workbook object to manipulate
  wb <- config[24]
  wbobj <- loadWorkbook(wb)

  # Extract the full data from the sheet
  data <- readxl::read_excel(wb, sheet = "YSI_Import") %>% 
    select(-1)
  # Find the row to paste the data on sheet 2, then copy the data over
  PasteRow <- as.numeric(NROW(read.xlsx(wb, sheet =  "ImportedToWQDB", colNames = F, cols = 1)) + 1)
  openxlsx::writeData(wbobj, sheet = 2, data, startCol = 1, startRow = PasteRow, colNames = F)
  
  # Find the last row of data to delete on sheet 1, delete the data, then save the workbook
  EndRow <- NROW(data) + 2
  openxlsx::deleteData(wbobj, sheet = 1, cols = 2:14, rows = 2:EndRow, gridExpand = T)
  openxlsx::saveWorkbook(wbobj, wb, overwrite = TRUE)
  print("Finished updating Field Parameter Importer excel file")
}

#   PROCESSING FUNCTION    #
############################.

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)

options(scipen = 999) # Eliminate Scientific notation in numerical fields
# Path to raw data file
path <- paste0(rawdatafolder,"/", file)
# Prep the raw data - send to importer worksheet
df <- PREP_DATA(file = path)

# Connect to db for queries below
dsn <- "DCR_DWSP_App_R"
database <- "DCR_DWSP"
schema <- 'Wachusett'
tz <- 'UTC'

con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)

### Add missing columns ####
# Get columns from table in db
cols <- dbListFields(con, schema_name = schema, name = "tblTribFieldParameters")
# Load tblParameters to access abbreviation for Unique ID
params <- dbReadTable(con, Id(schema = schema, table = "tblParameters"))

# Add the ones that are missing
for (col in cols) {
  if(!col %in% names(df)) {
    df <- mutate(df, "{col}" := NA)
  }
}
### Order columns to match db ####

df <- df[,cols]

if(identical(names(df), cols)) {
  print("Dataframe columns match...ready to proceed...")
} else {
  stop("Columns of the dataframe do not match data")
}

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

ratings <- dbReadTable(con, Id(schema = schema, table = "tblRatings"))

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
df$DataSource <-  paste0("Field_Parameters_", min(as_date(df$DateTimeET)),"_", max(as_date(df$DateTimeET)))

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
            # df     <- dfs[[1]]
            # path  <- dfs[[2]]

##################################################.

############################.
# Write data to Database ####
############################.

IMPORT_DATA <- function(df.wq, df.flags = NULL , path, file, filename.db ,processedfolder, ImportTable, ImportFlagTable = NULL){
  start <- now()
  print(glue("Starting data import at {start}"))
  # Import the data to the database
  dsn <- "DCR_DWSP_App_R"
  database <- "DCR_DWSP"
  schema <- "Wachusett"
  tz <- 'America/New_York'
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)

  odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportTable}")), value = df.wq, append = TRUE)

  ### Move Field Parameter csv files to the processed data folder ####
  
  rawYSI <- config[25]
  filelist <- list.files(rawYSI,".csv$")
  if (length(filelist) > 0) {
    print("Moving staged field parameter csv files to the imported folder...")
    filelist2 <- paste0(rawYSI,"/", filelist)
    file.rename(filelist2, paste0(config[26],"/", filelist))
  } else {
    print("There were no csv files associated with this field parameter data")
  }

    # Flag data
  if (class(df.flags) == "data.frame"){ # Check and make sure there is flag data to import 
    odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportFlagTable}")), value = df.flags, append = TRUE)
  } else {
    print("There were no flags to import")
  }
  
  # Disconnect from db and remove connection obj
  dbDisconnect(con)
  rm(con)
  
  UPDATE_IMPORTER_XL()
  end <- now()
  return(print(glue("Import finished at {end}, \n elapsed time {round(end - start)} seconds")))  
 }


#IMPORT_DATA(df, df.flags, path, file, filename.db, processedfolder = NULL, ImportTable, ImportFlagTable = NULL)
### END
 