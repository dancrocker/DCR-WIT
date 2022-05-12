##############################################################################################################################
#     Title: ImportProfiles_Q.R
#     Description: This script will process/import reservoir Profile Data from YSI EXO to database - adapted from Wachusett script
#     Written by: Dan Crocker, Max Nyquist, Brett Boisjolie
#     Last Update: May 12, 2022
#
##############################################################################################################################

# library(tidyverse)
# library(stringr)
# library(odbc)
# library(DBI)
# library(lubridate)
# library(magrittr)
# library(readxl)
# library(DescTools)
# library(glue)

###############################################################################################
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)

# Eliminate Scientific notation in numerical fields
options(scipen = 999)

# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)

# Read in the raw data - defaults to the last sheet added
df.wq <- read.csv(path, header = TRUE)   

# Process column names
names(df.wq)[names(df.wq) == 'Â.C.20E100861'] <- 'TWA-C'
names(df.wq)[names(df.wq) == 'X.C.20E100861'] <- 'TWA-C'
names(df.wq)[names(df.wq) == 'X.C.20E100862'] <- 'TWA-C'
names(df.wq)[names(df.wq) == 'DO...20F000962'] <- 'LDOs'
names(df.wq)[names(df.wq) == 'DO...20F000963'] <- 'LDOs'
names(df.wq)[names(df.wq) == 'DO.mg.L.20F000963'] <- 'LDOd'
names(df.wq)[names(df.wq) == 'DO.mg.L.20F000962'] <- 'LDOd'
names(df.wq)[names(df.wq) == 'SPC.uS.cm.20E100861'] <- 'SPCD'
names(df.wq)[names(df.wq) == 'SPC.uS.cm.20E100862'] <- 'SPCD'
names(df.wq)[names(df.wq) == 'pH.21B103444'] <- 'pH'
names(df.wq)[names(df.wq) == 'pH.20G100082'] <- 'pH'
names(df.wq)[names(df.wq) == 'FNU.20F000994'] <- 'TUR-FNU'
names(df.wq)[names(df.wq) == 'FNU.20F000995'] <- 'TUR-FNU'
names(df.wq)[names(df.wq) == 'FNU.22A101407'] <- 'TUR-FNU'
names(df.wq)[names(df.wq) == 'Phycocyanin.RFU.20F160828'] <- 'BGA-PC-RFU'
names(df.wq)[names(df.wq) == 'Phycocyanin.ug.L.20F160828'] <- 'BGA-PC'
names(df.wq)[names(df.wq) == 'Chlorophyll.RFU.20F160828'] <- 'Chl-RFU'
names(df.wq)[names(df.wq) == 'Chlorophyll.ug.L.20F160828'] <- 'Chl'
names(df.wq)[names(df.wq) == 'TAL.PC.RFU.20F160828'] <- 'BGA-PC-RFU'
names(df.wq)[names(df.wq) == 'TAL.PC.RFU.20F160829'] <- 'BGA-PC-RFU'
names(df.wq)[names(df.wq) == 'TAL.PC.ug.L.20F160829'] <- 'BGA-PC'
names(df.wq)[names(df.wq) == 'TAL.PC.ug.L.20F160828'] <- 'BGA-PC'
names(df.wq)[names(df.wq) == 'Chl.RFU.20F160828'] <- 'Chl-RFU'
names(df.wq)[names(df.wq) == 'Chl.RFU.20F160829'] <- 'Chl-RFU'
names(df.wq)[names(df.wq) == 'Chl.ug.L.20F160828'] <- 'Chl'
names(df.wq)[names(df.wq) == 'Chl.ug.L.20F160829'] <- 'Chl'
names(df.wq)[names(df.wq) == 'Vpos.m.20F160075'] <- 'DEP'
names(df.wq)[names(df.wq) == 'Vpos.m.20E102677'] <- 'DEP'
names(df.wq)[names(df.wq) == 'ï..Date'] <- 'Date'

#Remove RFU values and unwanted columns - collecting RFU data in 2022 and beyond
#if("TAL.PC.RFU.20F160828" %in% names(df.wq)) {
#df.wq <-  subset(df.wq, select=-c(TAL.PC.RFU.20F160828))
#}
#if("Chl.RFU.20F160828" %in% names(df.wq)) {
#df.wq <- subset(df.wq, select=-c(Chl.RFU.20F160828))
#}
# df.wq <- subset(df.wq, select=-c(X))
# df.wq <- subset(df.wq, select=-c(X.1))
# df.wq <- subset(df.wq, select=-c(X.2))
# df.wq <- subset(df.wq, select=-c(X.3))

# Data class/formats
df.wq$DateTimeET <- as.POSIXct(paste(as.Date(df.wq$Date, format ="%m/%d/%Y"), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M", tz = "America/New_York", usetz = T)

# reformat the Profile data to "Tidy" data format ("Long" instead of "Wide")
df.wq <- gather(df.wq, Parameter, Result, c(6:15))

#Get rid of unnecessary fields
df.wq <- df.wq[, c(3:4, 6:9)]

df.wq$Result <- round(as.numeric(df.wq$Result), 3)
df.wq$DEP <- round(as.numeric(df.wq$DEP),3)

# Connect to db for queries below
### Connect to Database   
dsn <- filename.db
database <- "DCR_DWSP"
schema <- "Quabbin"
tz <- 'America/New_York'
con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)

params <- dbReadTable(con,  Id(schema = "Wachusett", table = "tblParameters"))

#Standardize header
df.wq$Depth_m <- df.wq$DEP
df.wq$FinalResult <- df.wq$Result
df.wq$Station <- df.wq$Site
df.wq$Probe_Type <- df.wq$'Unit.ID'

#Standardize sites names
df.wq$Station <- as.character(df.wq$Station)
df.wq$Station[df.wq$Station == "den hill"] <- "DEN"

#Rearrange 
df.wq <- df.wq[, c(9,4,5,7,8,10)]

# UniqueID
df.wq$UniqueID <- ""
df.wq$UniqueID <- paste(df.wq$Station, format(df.wq$DateTimeET, format = "%Y-%m-%d %H:%M"), df.wq$Depth_m, df.wq$Parameter, df.wq$Probe_Type, sep = "_")

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

###############################################################################################

###############################################################################################

### DataSource
df.wq <- df.wq %>% mutate(DataSource = file)

### DataSourceID
# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(DateTimeET, Station)),]

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
df.wq$Parameter <- params$ParameterName[match(df.wq$Parameter, params$ParameterAbbreviation)]
df.wq$Units <- params$ParameterUnits[match(df.wq$Parameter, params$ParameterName)]

# Reorder remaining columns to match the database table exactly
df.wq <- df.wq[, c(11,1:5,12,6:10)]

# Get column names from db table
cnames <- dbListFields(con, ImportTable)
#list(cnames)
names(df.wq) <- cnames

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

########################################################################################################

##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
#df.flags is an optional argument  - not used for this dataset
  start <- now()
  print(glue("Starting data import at {start}"))
# Establish db connection
 dsn <- filename.db
 database <- "DCR_DWSP"
 schema <- 'Quabbin'
 tz <- 'America/New_York'

 con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)  # Get Import Table Columns
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

 end <- now()
 return(print(glue("Import finished at {end}, \n elapsed time {round(end - start)} seconds")))
}
### END

#IMPORT_DATA(df.wq, df.flags = NULL, path, file, filename.db, processedfolder = NULL,
#            ImportTable = ImportTable, ImportFlagTable = NULL)

