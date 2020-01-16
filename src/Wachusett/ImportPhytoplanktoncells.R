##############################################################################################################################
#     Title: ImportPhytoplanktoncells.R
#     Description: This script will Format/Process MWRA data to DCR 
#     Written by: Dan Crocker, Max Nyquist
#     Last Update: April 2018
#     This script will process and import Wachusett Phytoplankton data into the AB Database
##############################################################################################################################

# NOTE - THIS SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
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

#################
# INITIAL SETUP # 
#################

# scriptname <- "ImportPhytoplanktoncells.R"
# config <- ### Load Config from local file
# config <- as.character(config$CONFIG_VALUE)
# dataset <-  read_excel(config[8], sheet = 1, col_names = T, trim_ws = T) %>%
# filter(ImportMethod == "Importer-R" & ScriptProcessImport == scriptname)
# # Choose the dataset from options:
# dataset <- slice(dataset,1)
# 
# ### Function Arguments:
# rawdatafolder <- paste0(dataset[10])
# processedfolder <- paste0(dataset[11])
# filename.db <- paste0(dataset[6])
# ImportTable <- paste0(dataset[7])
# ImportFlagTable <- NULL # This data has no related flag table
# 
# ### Find the file to Import
# files <- grep(
#   x = list.files(rawdatafolder, ignore.case = T, include.dirs = F),
#   pattern = "^(?=.*\\b(xlsx|xlsm)\\b)(?!.*\\$\\b)", # regex to show xlsx files, but filter out lockfiles string = "$"
#   value = T,
#   perl =T
# )
# 
# #  List the files:
# files
# 
# # Select the file to import manually
# file <- files[7]

######################################################################################################################
######################################################################################################################
######################################################################################################################

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY! 

#############################

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)
  
  # filecheck <- grep(
  #   x = file,
  #   pattern = "Cellcounts_20", # regex to show xlsx files, but filter out lockfiles string = "$"
  #   value = T,
  #   perl =T
  # )
  # 
  # if (length(filecheck) == 0){
  #   stop(paste("You did not pick a valid file to import, please select a different file"), call. = FALSE)
  # }

# Eliminate Scientific notation in numerical fields
  options(scipen = 50) 

# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)

# Assign the sheet number
sheetNum <- as.numeric(length(excel_sheets(path)))

# Assign the sheet name
sheetName <- excel_sheets(path)[sheetNum] 

# Read in the raw data - defaults to the last sheet added
df.wq <- read_excel(path, sheet = sheetNum, range = cell_cols("AJ:AN"),  col_names = F, trim_ws = T, na = "nil") %>% 
  as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work

# Scan row 9 (Effort) for values, then reduce vector where effort is 0 or NA - these are column #s we want to remove
delcols <- as_vector(paste0(df.wq[9,]))
delcols <- which(delcols == "0"| delcols == "NA")

# Remove unwanted columns, discard first 4 rows, filter out empty rows (NA inf column 1), add a new row
df.wq <- df.wq %>%
  select(-delcols) %>% 
  slice(5:n()) %>% 
  filter(!is.na(X__1)) 

# Transpose the entire dataframe and convert back to Dataframe from Matrix
df.wq <- as.data.frame(t(df.wq)) 

# Rename Columns using first row values and then remove the first row
names(df.wq) <- unlist(df.wq[1,])
df.wq <- df.wq[-1,]

# Gather data records into individual rows for each taxa, then spread the datatype back up into columns
df.wq <- gather(df.wq,"taxa","cellcount",9:83, na.rm =T) 
  #na.rm = T to remove records with no count data 

# Data class/formats
df.wq$Date <- as.numeric(as.character(df.wq$Date))
df.wq$Date <- XLDateToPOSIXct(df.wq$Date)
df.wq$cellcount <- round(as.numeric(df.wq$cellcount),3)

# Drop unused levels from factor variables
df.wq[, 2:8] <- droplevels(df.wq[, 2:8])

# Fix factor vars that should be numeric
df.wq$`Depth (m)` <- as.numeric(as.character(df.wq$`Depth (m)`))
df.wq$Magnification <- as.numeric(as.character(df.wq$Magnification))


####Add new columns###
# Connect to db for queries below
con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}",
                                            paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                 timezone = "America/New_York")

# Get Taxa Table and check to make sure taxa in df.wq are in the Taxa Table - if not warn and exit
df_taxa_wach <- dbReadTable(con,"tbl_Taxa")
unmatchedTaxa <- which(is.na(df_taxa_wach$ID[match(df.wq$taxa, df_taxa_wach$Name)]))
length(unmatchedTaxa)
if (length(unmatchedTaxa) > 0){
  # Exit function and send a warning to user
  stop(paste("This data file contains", length(unmatchedTaxa),
             "records with taxa that are not present in the Taxa Table - 
             Please add new taxa to the Taxa Table or fix names prior to importing new records.",
             "The taxa not in the Taxa Table are: ", paste(unique(df.wq[unmatchedTaxa, "taxa"]), collapse = ", ")), call. = FALSE)
}

#double check taxa table Unique id in Phytoimporter

## Unique ID number
df.wq$UniqueID <- ""
df.wq$UniqueID <- paste(df.wq$Location, df.wq$Date, df.wq$`Depth (m)`, df_taxa_wach$ID[match(df.wq$taxa, df_taxa_wach$Name)], sep = "_")

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

Uniq <- dbGetQuery(con,paste0("SELECT UniqueID, ID FROM ", ImportTable))
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
df.wq <- df.wq[with(df.wq, order(Date, taxa, `Depth (m)`)),]

# Assign the numbers
df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)

### Importdate (Date)
df.wq$ImportDate <- Sys.Date() %>% as.Date()

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


df.wq <- df.wq[, c(15, 1:10, 14, 12, 13, 11)]
# Reorder remaining 30 columns to match the database table exactly
cnames <- dbListFields(con, ImportTable)
names(df.wq) <- cnames
# Create a list of the processed datasets
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- path
dfs[[3]] <- NA # Removed condition to test for flags and put it in the setFlagIDS() function

# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION



#### COMMENT OUT SECTION BELOW WHEN RUNNING SHINY
########################################################################################################
#RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = NULL )
# 
# # Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
########################################################################################################

##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
  # df.flags is an optional argument  - not used for this dataset
  
  # Establish db connection
  con <-  odbcConnectAccess(filename.db)
  # Get Import Table Columns
  ColumnsOfTable <- sqlColumns(con, ImportTable)
  
  # Set variable types
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME) 
  sqlSave(con, df.wq, tablename = ImportTable, append = T, 
          rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)
  
  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)
  
  return("Import Successful")
}
### END 

# IMPORT_DATA(df.wq, df.flags = NULL, path, file, filename.db, processedfolder = NULL, 
#             ImportTable = ImportTable, ImportFlagTable = NULL)

