##############################################################################################################################
#     Title: ImportTribs_Q.R
#     Description: This script will process/import Quabbin Trib Field Parameter Data from YSI ProQuatro to database
#     Written by: Brett Boisjolie
#     Last Update: August 2021
#
##############################################################################################################################

#library(glue)

###############################################################################################
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)
  
  # Eliminate Scientific notation in numerical fields
  options(scipen = 50) 
  
  # Get the full path to the file
  path <- paste0(rawdatafolder,"/", file)
  
  # Read in the raw data - defaults to the last sheet added
  YSI.df <- read.csv(path, header=TRUE)
  #YSI.df  <- read_excel(path, sheet= 1, col_names = T, trim_ws = T, na = "nil") %>%
  #  as.data.frame() 
  
  # Data class/formats
  YSI.df$Timestamp <- as.POSIXct(paste(mdy(YSI.df$Date), YSI.df$Time, sep = " "), format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York", usetz = T)
  #YSI.df$Timestamp <- as.POSIXct(paste(mdy(YSI.df$ï..Date), YSI.df$Time, sep = " "), format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York", usetz = T)
  
  # Drop unnecesary columns from DF - these columns are not needed for the database
  #YSI.df <- YSI.df %>% select(-c(Date, Time, pH.mV, B.pH, B.pH.mV, Pressure.mmHg.))
  YSI.df <- YSI.df %>% select(-c(Date, Time))
  #YSI.df <- YSI.df %>% select(-c(ï..Date, Time))
  
  #Change our parameter names to match DB parameter abbreviations
  YSI.df <-  plyr::rename(YSI.df,
                          c("Temp.C." = "TWA-C"))  
  YSI.df <- rename(YSI.df, c("LDOs" = "DO..."))  
  YSI.df <- rename(YSI.df, c("LDOd" = "DO.mg.L.")) 
  YSI.df <- rename(YSI.df, c("SPCD" = "SPC.uS.cm."))
  YSI.df <- rename(YSI.df, c("Station" = "Site"))
  YSI.df <- rename(YSI.df, c("DataSource" = "DataID"))
  YSI.df$Probe_Type <- "YSI PRO QUATRO"
  
  # reformat the Quabbin Trib field data to "Tidy" data format ("Long" instead of "Wide")
  YSI.df <- gather(YSI.df, Parameter, FinalResult, c("SPCD","LDOd", "LDOs", "pH","TWA-C"))
  YSI.df$FinalResult <- round(as.numeric(YSI.df$FinalResult), 2)
  
  #Create a UniqueID field that gathers data from different fields together to be unique for each record
  YSI.df$UniqueID <- paste(YSI.df$Station, YSI.df$Timestamp, YSI.df$Parameter,  sep = "_")
  
  # Connect to db for queries below
  ### Connect to Database   
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- "Quabbin"
  tz <- 'America/New_York'
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[35], timezone = tz)
  
  df_param <- dbReadTable(con,  Id(schema = "Wachusett", table = "tblParameters"))
  YSI.df$Parameter <- df_param$ParameterName[match(YSI.df$Parameter, df_param$ParameterAbbreviation)]
  # Now we use the match function to create the field "Units", populating this field with the units corresponding to the
  # record's parameter name
  YSI.df$Units <- df_param$ParameterUnits[match(YSI.df$Parameter, df_param$ParameterName)]
  
  ## Make sure it is unique within the data file - if not then exit function and send warning
  dupecheck <- which(duplicated(YSI.df$UniqueID))
  dupes <- YSI.df$UniqueID[dupecheck] # These are the dupes
  
  if (length(dupes) > 0){
    # Exit function and send a warning to userlength(dupes) # number of dupes
    stop(paste("This data file contains", length(dupes),
               "records that appear to be duplicates. Eliminate all duplicates before proceeding.",
               "The duplicate records include:", paste(head(dupes, 15), collapse = ", ")), call. = FALSE)
  }
  
  Uniq <- dbGetQuery(con, glue("SELECT [UniqueID], [ID] FROM [{schema}].[{ImportTable}]"))
  dupes2 <- Uniq[Uniq$UniqueID %in% YSI.df$UniqueID,]
  
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
  YSI.df <- YSI.df %>% mutate(DataSource = file)
  
  ### DataSourceID
  # Do some sorting first:
  YSI.df <- YSI.df[with(YSI.df, order(Timestamp, Station)),]
  
  # Assign the numbers
  YSI.df$DataSourceID <- seq(1, nrow(YSI.df), 1)
  
  ### Importdate (Date)
  YSI.df$ImportDate <- today()
  
  
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
    YSI.df$ID <- seq.int(nrow(YSI.df)) + ID.max.wq }
  
  YSI.df$ID <- setIDs()
  
  
  # Reorder remaining columns to match the database table exactly
  YSI.df <- YSI.df[, c("ID","Station","Timestamp","Parameter","FinalResult","Units","Probe_Type", "UniqueID","DataSource","DataSourceID","ImportDate")]
  
  df.wq <-  YSI.df %>%
    rename(
      Site = Station,
      DateTimeET = Timestamp)

  # Create a list of the processed datasets
  dfs <- list()
  dfs[[1]] <- df.wq
  dfs[[2]] <- path
  dfs[[3]] <- NULL # Removed condition to test for flags and put it in the setFlagIDS() function
  
  # Disconnect from db and remove connection obj
  dbDisconnect(con)
  rm(con)
  return(dfs) 
    
}

# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, probe, ImportTable = ImportTable, ImportFlagTable = NULL )

# Extract each element needed
# df.wq     <- dfs[[1]]
# path      <- dfs[[2]]
# df.flags  <- dfs[[3]]

##############################################################################
####READ RECORD TO SQL

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


#IMPORT_DATA(df.wq, df.flags = NULL, path, file, filename.db, processedfolder = NULL,
#            ImportTable = ImportTable, ImportFlagTable = NULL)
