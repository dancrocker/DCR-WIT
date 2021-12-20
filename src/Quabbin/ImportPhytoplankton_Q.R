##############################################################################################################################
#     Title: ImportPhytoplankton_Q.R
#     Description: This script will process plankton enumeration datasheets for import into the Quabbin Database
#     Written by: Brett Boisjolie, Dan Crocker, Kate Langley
#     Last Update: December 20, 2021
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
# library(testthat)
# library(glue)

# COMMENT OUT ABOVE CODE WHEN RUNNING IN SHINY!

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)
  
  # Eliminate Scientific notation in numerical fields
  options(scipen = 999)
  # Get the full path to the file
  path <- paste0(rawdatafolder,"/", file)
  
  # Assign the sheet number
  sheetNum <- as.numeric(length(excel_sheets(path)))
  # Assign the sheet name
  sheetName <- excel_sheets(path)[sheetNum]
  # Excel name
  file <- "A_phytoASU_2021.xlsx"
  # Read in the raw data - defaults to the last sheet added
  df.wq <- read_excel(path, sheet = sheetNum, range = cell_cols("B:S"),  col_names = F, trim_ws = T, na = "nil") %>%
    as.data.frame()   
  totals <- read_excel(path, sheet = sheetNum, range = cell_cols("V:AI"),  col_names = F, trim_ws = T, na = "nil") %>%
    as.data.frame()
  #Extract information on sampling
  dataDate <- as.character(df.wq[5,4])
  dataDate <-  mdy(dataDate)
  dataLoc <- as.character(df.wq[2,8])
  analyst <- as.character(df.wq[2,16])
  #ExtraFields < - as.character(df.wq[7,4])
  sampletype <- as.character(df.wq[6,4])  
  Microscope <- "OLYMPUS BX43 MICROSCOPE"
  #Magnification - field in DB
  #Method - field in DB
  
  #depths sampled
  dep1 <- as.numeric(df.wq[2,10])
  dep2 <- as.numeric(df.wq[3,10])
  dep3 <- as.numeric(df.wq[4,10])
  dep4 <- as.numeric(df.wq[5,10])
  
  #Drop out depths that are NA_real_
  #  dep1 <- dep1[!is.na(dep1)]
  #  dep2 <- dep2[!is.na(dep2)]
  #  dep3 <- dep3[!is.na(dep3)]
  #  dep4 <- dep4[!is.na(dep4)]
  
  #initial volume per depth
  initialvolume1 <- as.numeric(df.wq[2,12])
  initialvolume2 <- as.numeric(df.wq[3,12])
  initialvolume3 <- as.numeric(df.wq[4,12])
  initialvolume4 <- as.numeric(df.wq[5,12])
  #concentrated volume per depth
  concentratedvolume1 <- as.numeric(df.wq[2,14])
  concentratedvolume2 <- as.numeric(df.wq[3,14])
  concentratedvolume3 <- as.numeric(df.wq[4,14])
  concentratedvolume4 <- as.numeric(df.wq[5,14])
  #effort associated with each sample
  effort1 <- as.character(df.wq[2,18])
  effort2 <- as.character(df.wq[3,18])
  effort3 <- as.character(df.wq[4,18])
  effort4 <- as.character(df.wq[5,18])
  Effort <- c(effort1,effort2,effort3,effort4)
  Effort <- Effort[!is.na(Effort)]
  
  
  ###Process density data
  #Remove unwanted columns for density
  df.wq <- df.wq  %>% 
    select(c(2,6,10,14,18))    
  
  #Remove unwanted rows for density
  df.wq <- df.wq[-c(1:14,29,60,71,93,97,97:109), ]
  
  #Rename columns
  names(df.wq) <- c("Taxa","Density1", "Density2", "Density3", "Density4")
  
  #Add columns for depth
  df.wq["dep1"]<-dep1
  df.wq["dep2"]<-dep2  
  df.wq["dep3"]<-dep3 
  df.wq["dep4"]<-dep4 
  
  ### Add columns for other fields
  #initialvolume
  #concentratedvolume
  #Add columns for effort
  df.wq["effort1"]<-effort1
  df.wq["effort2"]<-effort2  
  df.wq["effort3"]<-effort3 
  df.wq["effort4"]<-effort4 
  
  #PROCESS TAXA TOTALS
  Depth_m <- c(dep1,dep2,dep3,dep4)
  
  #Total Diatoms
  Density <- totals[7:10,2]
  Diatoms <- data.frame(Density, Depth_m)
  Diatoms<-Diatoms[which(Diatoms$Density!= '-'),]
  Diatoms$Taxa <- "Total Diatoms"
  Diatoms$Effort <- Effort
  #Total Chlorophytes
  Density <- totals[7:10,3]
  Chlorophytes <- data.frame(Density, Depth_m)
  Chlorophytes<-Chlorophytes[which(Chlorophytes$Density!= '-'),]
  Chlorophytes$Taxa <- "Total Chlorophytes"
  Chlorophytes$Effort <- Effort
  #Total Chrysophytes
  Density <- totals[7:10,4]
  Chrysophytes <- data.frame(Density, Depth_m)
  Chrysophytes<-Chrysophytes[which(Chrysophytes$Density!= '-'),]
  Chrysophytes$Taxa <- "Total Chrysophytes" 
  Chrysophytes$Effort <- Effort
  #Total Chlorophytes
  Density <- totals[7:10,9]
  Cyanophytes <- data.frame(Density, Depth_m)
  Cyanophytes<-Cyanophytes[which(Cyanophytes$Density!= '-'),]
  Cyanophytes$Taxa <- "Total Cyanophytes" 
  Cyanophytes$Effort <- Effort
  #Total Dinoflagellates
  Density <- totals[7:10,12]
  Dinoflagellates <- data.frame(Density, Depth_m)
  Dinoflagellates<-Dinoflagellates[which(Dinoflagellates$Density!= '-'),]
  Dinoflagellates$Taxa <- "Total Dinoflagellates"
  Dinoflagellates$Effort <- Effort
  #Total Other
  #  Density <- totals[7:10,13]
  #  Other <- data.frame(Density, Depth_m)
  #  Other <- Other[which(Other$Density!= '-'),]
  #  Other$Taxa <- "Total Other"
  #  Other$Effort <- Effort
  #Total Cyanophytes
  Density <- totals[7:10,6]
  Dinobryon <- data.frame(Density, Depth_m)
  Dinobryon <-Dinobryon[which(Dinobryon$Density!= '-'),]
  Dinobryon$Taxa <- "Total Dinobryon"
  Dinobryon$Effort <- Effort
  #Total Grand Totals
  Density <- totals[7:10,14]
  Grand <- data.frame(Density, Depth_m)
  Grand <- Grand[which(Grand$Density!= '-'),]
  Grand <- Grand[which(Grand$Density!= '0'),]
  Grand$Taxa <- "Grand Total"  
  Grand$Effort <- Effort
  
  Totals <- rbind(Diatoms,Chlorophytes,Chrysophytes,Cyanophytes,Dinoflagellates,Dinobryon,Grand)
  Totals <- Totals[,c(3,1,2,4)]
  Totals$Density <- Totals$Density %>% as.character() %>% as.numeric()
  
  df.wq$Density1 <- as.numeric(df.wq$Density1)
  df.wq$Density2 <- as.numeric(df.wq$Density2)
  df.wq$Density3 <- as.numeric(df.wq$Density3)
  df.wq$Density4 <- as.numeric(df.wq$Density4)
  
  df.wq <- df.wq %>% 
    mutate(Density1 = ifelse(is.na(Density1),0,Density1),
           Density2 = ifelse(is.na(Density2),0,Density2),
           Density3 = ifelse(is.na(Density3),0,Density3),
           Density4 = ifelse(is.na(Density4),0,Density4)
    )
  
  # Remove unwanted columns
  #!# Split data frame into two based on depth and merge into one
  df1.wq <- df.wq %>%
    select(c(1,2,6,10))
  names(df1.wq) <- c("Taxa","Density","Depth_m","Effort")
  #Drop out empty columns where there is NA or 0 columns
  df1.wq <- df1.wq[colSums(is.na(df1.wq) | df1.wq == 0) != nrow(df1.wq)]
  
  if(max(df.wq$Density2, na.rm = TRUE) <= 0) {
    df2.wq <- NULL
  } else {
    df2.wq <- df.wq %>%
      select(c(1,3,7,11))
    names(df2.wq) <- (c("Taxa","Density","Depth_m","Effort"))
    df2.wq <- df2.wq[colSums(is.na(df2.wq) | df2.wq == 0) != nrow(df2.wq)]
  }
  
  if(max(df.wq$Density3, na.rm = TRUE) <= 0) {
    df3.wq <- NULL
  } else { 
    df3.wq <- df.wq %>%
      select(c(1,4,8,12))
    names(df3.wq) <- c("Taxa","Density","Depth_m","Effort")
    df3.wq <- df3.wq[colSums(is.na(df3.wq) | df3.wq == 0) != nrow(df3.wq)]
  }
  
  if(max(df.wq$Density4, na.rm = TRUE) <= 0) {
    df4.wq <- NULL
  } else { 
    df4.wq <- df.wq %>%
      select(c(1,5,9,13))
    names(df4.wq) <- (c("Taxa","Density","Depth_m","Effort"))
    df4.wq <- df4.wq[colSums(is.na(df4.wq) | df4.wq == 0) != nrow(df4.wq)]
  }
  
  #Bind dataframes
  df.wq <- rbind(df1.wq, df2.wq)
  df.wq <- rbind(df.wq, df3.wq)
  df.wq <- rbind(df.wq, df4.wq)
  
  #!# Remove rows where Density = NA
  removeNA <- complete.cases(df.wq)
  df.wq <- df.wq[removeNA,]
  
  #!# Remove values where Density = 0
  df.wq <- df.wq[df.wq$Density > 0,]
  
  #MERGE TOTALS TO DF
  df.wq <- rbind(Totals, df.wq)
  
  # modify column names and add missing columns
  
  df.wq <- df.wq %>% 
    mutate(SampleDate = dataDate, 
           Station = dataLoc, 
           Analyst = analyst, 
           ImportDate = today(), 
           DataSource = file,
           Microscope = Microscope) 
  
  
  # Fix data types and digits
  df.wq$Density <- round(df.wq$Density, digits=3)
  
  # Connect to db for queries below
  ### Connect to Database   
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- "Quabbin"
  tz <- 'America/New_York'
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)
  
  # Get Taxa Table and check to make sure taxa in df.wq are in the Taxa Table - if not warn and exit
  df_taxa <- dbReadTable(con, Id(schema = "Wachusett", table = "tbl_PhytoTaxa"))
  unmatchedTaxa <- which(is.na(df_taxa$ID[match(df.wq$taxa, df_taxa$Phyto_Name)]))
  
  if (length(unmatchedTaxa) > 0){
    # Exit function and send a warning to user
    stop(paste("This data file contains", length(unmatchedTaxa),
               "records with taxa that are not present in the Taxa Table -
             Please add new taxa to the Taxa Table or fix names prior to importing new records.",
               "The taxa not in the Taxa Table are: ", paste(unique(df.wq[unmatchedTaxa, "taxa"]), collapse = ", ")), call. = FALSE)
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
  
  # Do some sorting:
  df.wq <- df.wq[with(df.wq, order(SampleDate, Taxa, Depth_m)),]
  
  # Assign the DataSourceID
  df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)
  
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
  
  # Reorder columns to match the database table exactly
  df.wq$PA_Value <- ""
  df.wq$Method <- ""
  df.wq$Magnification <- ""
  df.wq <-df.wq[,c(13,5,6,3,7,4,10,16,15,1,2,14,8,9, 12, 11)]
  
  
  # Reorder columns to match the database table exactly ####
  col.order.wq <- dbListFields(con, schema_name = schema, name = ImportTable)
  df.wq <-  df.wq[,col.order.wq]
  
  ######!############!###########!##########
  
  #Change data type for field to match database
  df.wq$Magnification <- as.numeric(df.wq$Magnification)
  df.wq$Density <- as.numeric(df.wq$Density)
  df.wq$PA_Value <- as.numeric(df.wq$PA_Value)
  
  
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
#RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
#dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, ImportTable = ImportTable, ImportFlagTable = NULL )
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
  start <- now()
  print(glue("Starting data import at {start}"))
  ### CONNECT TO DATABASE ####
  ### Set DB
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- 'Quabbin'
  tz <- 'America/New_York'
  ### Connect to Database 
  con <- dbConnect(odbc::odbc(), dsn, uid = dsn, pwd = config[35], timezone = tz)
  
  DBI::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.{ImportTable}")), value = df.wq, append = TRUE, row.names =  FALSE)
  
  # Disconnect from db and remove connection obj
  dbDisconnect(con)
  rm(con)
  
  end <- now()
  return(print(glue("Import finished at {end}, \n elapsed time {round(end - start)} seconds")))
}

#IMPORT_DATA(df.wq = df.wq, df.flags = NULL, path, file, filename.db, processedfolder = NULL,
#            ImportTable = ImportTable, ImportFlagTable = NULL)

