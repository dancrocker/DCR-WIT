##############################################################################################################################
#     Title: ImportPhyto.R
#     Description: This script will Format/Process Quabbin reservoir phytoplankton data
#     Written by: Dan Crocker, Max Nyquist, Brett Boisjolie
#     Last Update: June 2018
#    This script will process and import Quabbin Phytoplankton data into the database
##############################################################################################################################

# COMMENT OUT BELOW WHEN RUNNING FUNCTION IN SHINY

### Load libraries needed ####
  # library(tidyverse)
  # library(stringr)
  # library(odbc)
  # library(RODBC)
  # library(DBI)
  # library(lubridate)
  # library(readxl)
  # library(magrittr)
  # library(DescTools)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)

# Eliminate Scientific notation in numerical fields
options(scipen = 999)
# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)
# Assign the sheet number
sheetNum <- as.numeric(length(excel_sheets(path)))
# Read in the raw data - defaults to the last sheet added
df.wq <- read_excel(path, sheet = sheetNum, range = cell_cols("C:I"),  col_names = F, trim_ws = T, na = "nil") %>%
                    as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work

dataDate <- as.Date(df.wq[1,2])
dataLoc <- as.character(df.wq[3,2])
analyst <- as.character(df.wq[1,5])
dep1 <- as.numeric(df.wq[3,4])
dep2 <- as.numeric(df.wq[3,7])

# Remove unwanted columns
df.wq <- df.wq  %>% 
  select(c(1,4,7)) 

# Rename Columns using applicable row values and then remove the first row and last rows
names(df.wq) <- unlist(df.wq[6,])
df.wq <- df.wq[-1:-7,]
df.wq <- df.wq[-c(60:64),]

#!# Add columns for depths
df.wq["dep1"]<-dep1
df.wq["dep2"]<-dep2

#!# Split data frame into two based on depth and merge into one
df1.wq <- df.wq %>%
  select(c(1,2,4))
names(df1.wq) <- (c("Taxa","Density","Depth_m"))

df2.wq <- df.wq %>%
  select(c(1,3,5))
names(df2.wq) <- (c("Taxa","Density","Depth_m"))

df.wq <- rbind(df1.wq, df2.wq)

#!# Remove values where Density = NA
removeNA <- complete.cases(df.wq)
df.wq <- df.wq[removeNA,]

#!# Remove values where Density = 0

df.wq <- df.wq[df.wq$Density > 0,]

# modify column names and add missing columns

df.wq <- df.wq %>% 
  mutate(SampleDate = dataDate, 
         Station = dataLoc, 
         Analyst = analyst, 
         ImportDate = today(), 
         DataSource = file) 


# Fix data types and digits
df.wq$Density <- round(as.numeric(df.wq$Density))
df.wq$Depth_m <- as.numeric(df.wq$Depth_m)
# Connect to db 

con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb, *.accdb)}",
                                            paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                 timezone = "America/New_York")
# Get Taxa Table and check to make sure taxa in df.wq are in the Taxa Table - if not warn and exit
df_taxa_wach <- dbReadTable(con,"tbl_Taxa")
 unmatchedTaxa <- which(is.na(df_taxa_wach$ID[match(df.wq$Taxa, df_taxa_wach$Name)]))
if (length(unmatchedTaxa) > 0){
  # Exit function and send a warning to user
  stop(paste("This data file contains", length(unmatchedTaxa),
             "records with taxa that are not present in the Taxa Table -
            Please add new taxa to the Taxa Table or fix names prior to importing new records.",
            "The taxa not in the Taxa Table are: ", paste(unique(df.wq[unmatchedTaxa, "Taxa"]), collapse = ", ")), call. = FALSE)
 }
# Unique ID number
df.wq$UniqueID <- paste(df.wq$Station, df.wq$SampleDate, df.wq$Depth_m, df.wq$Taxa, sep = "_")

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

# Do some sorting:
df.wq <- df.wq[with(df.wq, order(SampleDate, Taxa, Depth_m)),]

# Assign the DataSourceID
df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)

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

# Reorder columns to match the database table exactly
cnames <- dbListFields(con, ImportTable)
df.wq <- df.wq[,cnames]
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

#### COMMENT OUT SECTION BELOW WHEN RUNNING SHINY
########################################################################################################
# #RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = NULL )
# # #
# # # # Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]

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
# 
# IMPORT_DATA(df.wq, df.flags = NULL, path, file, filename.db, processedfolder = NULL,
#              ImportTable = ImportTable, ImportFlagTable = NULL)




