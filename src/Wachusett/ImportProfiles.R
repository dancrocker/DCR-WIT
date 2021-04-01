###############################  HEADER  ######################################
#  TITLE: ImportProfiles.R
#  DESCRIPTION: This script will process/import reservoir Profile Data to database
#  AUTHOR(S): Dan Crocker, Max Nyquist
#  DATE LAST UPDATED: 2021-03-31
#  GIT REPO: 
#  R version 3.6.0 (2019-04-26)  i386
##############################################################################.

# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

# library(tidyverse)
# library(stringr)
# library(odbc)
# library(RODBC)
# library(DBI)
# library(lubridate)
# library(magrittr)
# library(readxl)
# library(DescTools)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

########################################################################.
###                          Process Data                           ####
########################################################################.

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)

# Eliminate Scientific notation in numerical fields
options(scipen = 999)

# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)

# Assign the sheet number
sheetNum <- as.numeric(length(excel_sheets(path)))

# Assign the sheet name
sheetName <- excel_sheets(path)[sheetNum]

# Read in the raw data - defaults to the last sheet added
df.wq <- read_excel(path, sheet = sheetNum, range = cell_cols("A:R"),  col_names = F, trim_ws = T, na = "nil") %>%
  as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work

# Remove unwanted columns, discard first 4 rows, filter out empty rows (NA inf column 1), add a new row
df.wq <- df.wq %>%
  slice(5:n())
# Rename Columns using first row values and then remove the first row
names(df.wq) <- unlist(df.wq[1,])
df.wq <- df.wq[-1,]

df.wq <- df.wq[complete.cases(df.wq[, 1:3]),]

# Data class/formats
df.wq$Date <- as.numeric(as.character(df.wq$Date))
df.wq$Date <- XLDateToPOSIXct(df.wq$Date)
df.wq$Time <- as.numeric(as.character(df.wq$Time))
df.wq$Time <- XLDateToPOSIXct(df.wq$Time)

#Get rid of Time formatting
df.wq <- separate(df.wq, Time, into = c("Date2", "Time"), sep = " ")

# reformat the Wachusett Profile data to "Tidy" data format ("Long" instead of "Wide")
df.wq <- gather(df.wq, Parameter, Result, c(7:16))

# DateTimeET
df.wq$DateTimeET <- as.POSIXct(paste(as.Date(df.wq$Date, format ="%Y-%m-%d"), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = T)

#Get rid of UserID, Date,Time and Secchi
df.wq <- df.wq[, c(4,12, 10, 7, 11, 5)]

df.wq$Result <- round(as.numeric(df.wq$Result), 3)
df.wq$DEP <- round(as.numeric(df.wq$DEP),3)

# Connect to database
dsn <- filename.db
database <- "DCR_DWSP"
schema <- "Wachusett"
tz <- 'UTC'
con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)

probes <- dbReadTable(con, Id(schema = schema, table = "tbl_Equipment"))
df_param <- dbReadTable(con, Id(schema = schema, table = "tblParameters"))

df.wq$`Unit ID` <- probe

# UniqueID
df.wq$UniqueID <- ""
df.wq$UniqueID <- paste(df.wq$Site, format(df.wq$DateTimeET, format = "%Y-%m-%d %H:%M"), df.wq$DEP, df.wq$Parameter, probes$EquipNum[match(df.wq$`Unit ID`, probes$EquipName)], sep = "_")

## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df.wq$UniqueID))
dupes <- df.wq$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste("This data file contains", length(dupes),
             "records that appear to be duplicates. Eliminate all duplicates before proceeding.",
             "The duplicate records include:", paste(head(dupes, 15), collapse = ", ")), call. = FALSE)
}

Uniq <- dbGetQuery(con, glue("SELECT [UniqueID], [ID] FROM [{schema}].[{ImportTable}]"))
dupes2 <- Uniq[Uniq$UniqueID %in% df.wq$UniqueID,]

if (nrow(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste("This data file contains", nrow(dupes2),
             "records that appear to already exist in the database!
             Eliminate all duplicates before proceeding.",
             "The duplicate records include:", paste(head(dupes2$UniqueID, 15), collapse = ", ")), call. = FALSE)
}
rm(Uniq)

### DataSource
df.wq <- df.wq %>% mutate(DataSource = paste(file, sheetName, sep = "_"))

### DataSourceID
# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(DateTimeET, Site)),]

# Assign the numbers
df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)

### Importdate (Date)
df.wq$ImportDate <- today()


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

# Change parameter to full name and add column for units
df.wq$Parameter <- df_param$ParameterName[match(df.wq$Parameter, df_param$ParameterAbbreviation)]
df.wq$Units <- df_param$ParameterUnits[match(df.wq$Parameter, df_param$ParameterName)]

# Reorder remaining 30 columns to match the database table exactly
df.wq <- df.wq[, c(11, 1:5, 12, 6:10)]

# Get column names from db table
cnames <- dbListFields(con, schema = schema, ImportTable)
#list(cnames)
names(df.wq) <- cnames

# change variable types to match database
df.wq$ID <- as.integer(df.wq$ID)
df.wq$DataSourceID <- as.integer(df.wq$DataSourceID)

# Change time to UTC in DateTimeET
df.wq$DateTimeET <- format(df.wq$DateTimeET, tz = "America/New_York", usetz = TRUE) %>%
  lubridate::as_datetime()

# Create a list of the processed datasets
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- path
dfs[[3]] <- NULL # Removed condition to test for flags and put it in the setFlagIDS() function

# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION

# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, probe, ImportTable = ImportTable, ImportFlagTable = NULL )

# Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]

########################################################################.
###                       Write Data to Database                    ####
########################################################################.

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
  # df.flags is an optional argument  - not used for this dataset

  # Establish db connection
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- 'Wachusett'
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

  return("Import Successful")
}
### END

IMPORT_DATA(df.wq, df.flags = NULL, path, file, filename.db, processedfolder = NULL,
            ImportTable = ImportTable, ImportFlagTable = NULL)
