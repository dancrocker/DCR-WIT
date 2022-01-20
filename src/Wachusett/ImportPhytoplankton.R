###############################  HEADER  ######################################
#  TITLE: ImportPhytoplankton.R
#  DESCRIPTION: This script will Format/Process MWRA data to DCR
#  AUTHOR(S): Dan Crocker, Max Nyquist
#  DATE LAST UPDATED: 2021-03-31
#  GIT REPO: 
#  R version 3.6.0 (2019-04-26)  i386
##############################################################################.

# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

# # Load libraries needed
# library(tidyverse)
# library(stringr)
# library(odbc)
# library(RODBC)
# library(DBI)
# library(lubridate)
# library(magrittr)
# library(readxl)
# library(magrittr)
# library(DescTools)
# library(devtools)
# library(tidyr)
# library(glue)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

########################################################################.
###                          PROCESS DATA                           ####
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
df.wq <- read_excel(path, sheet = sheetNum, range = cell_cols("AJ:AS"),  col_names = F, trim_ws = T, na = "nil") %>%
                    as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work

# Scan row 9 (Effort) for values, then reduce vector where effort is 0 or NA - these are column #s we want to remove
delcols <- as_vector(paste0(df.wq[9,]))
delcols <- which(delcols == "0"|delcols == "NA")

# Remove unwanted columns, discard first 4 rows, filter out empty rows (NA inf column A), add a new row

df.wq <- df.wq %>%
  select(-delcols) %>%
  slice(5:n()) 

names(df.wq) <- LETTERS[seq(from = 1, to = ncol(df.wq))]  

df.wq <- df.wq %>%
  dplyr::filter(!is.na(`A`)) %>%
  add_row()

# Count how many depths were sampled this day
cdepths <- (ncol(df.wq) - 1)/2

# Specify which columns are for phytoplankton counts
countcols <- 2:(cdepths + 1)

# Specify which columns are for Presence-Absence
PAcols <- (ncol(df.wq) - cdepths +1):ncol(df.wq)

# Add information to the last row of data which holds the count vs PA specification in a new variable "DataType" (once transposed)
df.wq[nrow(df.wq),1] <- "DataType"
df.wq[nrow(df.wq),countcols] <- "count"
df.wq[nrow(df.wq),PAcols] <- "PA"

# Transpose the entire dataframe and convert back to Dataframe from Matrix
df.wq <- as.data.frame(t(df.wq))

# Rename Columns using first row values and then remove the first row
names(df.wq) <- unlist(df.wq[1,])
df.wq <- df.wq[-1,]

# Gather data records into individual rows for each taxa, then spread the datatype back up into columns
df.wq <- gather(df.wq,"taxa","count", 10:ncol(df.wq)-1, na.rm =T) %>%
  spread(key = DataType, value = count)

# Data class/formats
df.wq$Date <- as.numeric(as.character(df.wq$Date))
df.wq$Date <- XLDateToPOSIXct(df.wq$Date)
df.wq$count <- round(as.numeric(df.wq$count),3)

# Drop unused levels from factor variables
df.wq[, 2:8] <- droplevels(df.wq[, 2:8])

# Fix factor vars that should be numeric or integer
df.wq$PA <- as.integer(df.wq$PA)
df.wq$`Depth (m)` <- as.numeric(as.character(df.wq$`Depth (m)`))
df.wq$Magnification <- as.integer(as.character(df.wq$Magnification))

### Add new columns ####

# Connect to db for queries below
database <- "DCR_DWSP"
dsn <- filename.db
schema <- "Wachusett"
tz <- 'America/New_York'
con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)
# 
# Get Taxa Table and check to make sure taxa in df.wq are in the Taxa Table - if not warn and exit
df_taxa_wach <- dbReadTable(con, Id(schema = schema, table = "tbl_PhytoTaxa"))
unmatchedTaxa <- which(is.na(df_taxa_wach$ID[match(df.wq$taxa, df_taxa_wach$Phyto_Name)]))

if (length(unmatchedTaxa) > 0){
  # Exit function and send a warning to user
  stop(paste("This data file contains", length(unmatchedTaxa),
             "records with taxa that are not present in the Taxa Table -
             Please add new taxa to the Taxa Table or fix names prior to importing new records.",
             "The taxa not in the Taxa Table are: ", paste(unique(df.wq[unmatchedTaxa, "taxa"]), collapse = ", ")), call. = FALSE)
}
# Unique ID number
df.wq$UniqueID <- ""
df.wq$UniqueID <- paste(df.wq$Location, df.wq$Date, df.wq$`Depth (m)`, df_taxa_wach$ID[match(df.wq$taxa, df_taxa_wach$Phyto_Name)], sep = "_")

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

### DataSourceID ####
# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(Date, taxa, `Depth (m)`)),]

# Assign the numbers
df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)

### Importdate (Date)
df.wq$ImportDate <- Sys.Date() %>% as.Date()

### IDs

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

df.wq <- df.wq[, c(16, 1:11, 15, 13, 14, 12)]
# Reorder remaining columns to match the database table exactly
cnames <- dbListFields(con, schema = schema, ImportTable)
names(df.wq) <- cnames

# change variable types to match database
df.wq$ID <- as.integer(df.wq$ID)
df.wq$DataSourceID <- as.integer(df.wq$DataSourceID)
df.wq$SampleDate <- as.Date(df.wq$SampleDate)

# Create a list of the processed datasets
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- path
dfs[[3]] <- NULL # Removed condition to test for flags and put it in the setFlagIDS() function

# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION ####

#### COMMENT OUT SECTION BELOW WHEN RUNNING SHINY
########################################################################################################.
# #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = NULL )
# #
# # # Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]


########################################################################.
###                       Write data to Database                    ####
########################################################################.

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
  # df.flags is an optional argument  - not used for this dataset

# Establish db connection
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- 'Wachusett'
  tz <- 'America/New_York'
  
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)# Get Import Table Columns

  ColumnsOfTable <- dbListFields(con, schema = schema, ImportTable)

# # Set variable types -- not necessary because specified above?
# varTypes  <- as.character(ColumnsOfTable$TYPE_NAME)
# sqlSave(con, df.wq, tablename = ImportTable, append = T,
#           rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)

# Import the data to the database
odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportTable}")), value = df.wq, append = TRUE)

# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)

  return("Import Successful")
}
### END

# IMPORT_DATA(df.wq = df.wq, df.flags = NULL, path, file, filename.db, processedfolder = NULL,
#             ImportTable = ImportTable, ImportFlagTable = NULL)




