##############################################################################################################################
#     Title: ImportTribs_Q.R
#     Description: This script will process/import Quabbin Trib Field Parameter Data from YSI ProQuatro to database
#     Written by: Brett Boisjolie
#     Last Update: Dec 19 2021
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
  df.wq <- read.csv(path, header=TRUE)
  #df.wq  <- read_excel(path, sheet= 1, col_names = T, trim_ws = T, na = "nil") %>%
  #  as.data.frame() 
  
  # Data class/formats
  df.wq$DateTimeET <- as.POSIXct(paste(mdy(df.wq$Date), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York", usetz = T)
  #df.wq$DateTimeET <- as.POSIXct(paste(mdy(df.wq$ï..Date), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York", usetz = T)
  
  # Drop unnecesary columns from DF - these columns are not needed for the database
  #df.wq <- df.wq %>% select(-c(Date, Time, pH.mV, B.pH, B.pH.mV, Pressure.mmHg.))
  df.wq <- df.wq %>% select(-c(Date, Time))
  #df.wq <- df.wq %>% select(-c(ï..Date, Time))
  
  #Change our parameter names to match DB parameter abbreviations
  df.wq <-  plyr::rename(df.wq,
                          c("Temp.C." = "TWA-C"))  
  df.wq <- rename(df.wq, c("LDOs" = "DO..."))  
  df.wq <- rename(df.wq, c("LDOd" = "DO.mg.L.")) 
  df.wq <- rename(df.wq, c("SPCD" = "SPC.uS.cm."))
  df.wq <- rename(df.wq, c("DataSource" = "DataID"))
  df.wq$Probe_Type <- "YSI PRO QUATRO"
  
  # reformat the Quabbin Trib field data to "Tidy" data format ("Long" instead of "Wide")
  df.wq <- gather(df.wq, Parameter, FinalResult, c("SPCD","LDOd", "LDOs", "pH","TWA-C"))
  df.wq$FinalResult <- round(as.numeric(df.wq$FinalResult), 2)
  
  #Create a UniqueID field that gathers data from different fields together to be unique for each record
  df.wq$UniqueID <- paste(df.wq$Site, df.wq$DateTimeET, df.wq$Parameter,  sep = "_")
  
  # Connect to db for queries below
  ### Connect to Database   
  dsn <- filename.db
  database <- "DCR_DWSP"
  schema <- "Quabbin"
  tz <- 'America/New_York'
  pool <- dbPool(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)
  
  
  df_param <- dbReadTable(pool,  Id(schema = "Wachusett", table = "tblParameters"))
  df.wq$Parameter <- df_param$ParameterName[match(df.wq$Parameter, df_param$ParameterAbbreviation)]
  # Now we use the match function to create the field "Units", populating this field with the units corresponding to the
  # record's parameter name
  df.wq$Units <- df_param$ParameterUnits[match(df.wq$Parameter, df_param$ParameterName)]
  
  ## Make sure it is unique within the data file - if not then exit function and send warning
  dupecheck <- which(duplicated(df.wq$UniqueID))
  dupes <- df.wq$UniqueID[dupecheck] # These are the dupes
  
  if (length(dupes) > 0){
    # Exit function and send a warning to userlength(dupes) # number of dupes
    stop(paste("This data file contains", length(dupes),
               "records that appear to be duplicates. Eliminate all duplicates before proceeding.",
               "The duplicate records include:", paste(head(dupes, 15), collapse = ", ")), call. = FALSE)
  }
  
  Uniq <- dbGetQuery(pool, glue("SELECT [UniqueID], [ID] FROM [{schema}].[{ImportTable}]"))
  dupes2 <- Uniq[Uniq$UniqueID %in% df.wq$UniqueID,]
  
  if (nrow(dupes2) > 0){
    # Exit function and send a warning to user
    stop(paste("This data file contains", nrow(dupes2),
               "records that appear to already exist in the database!
             Eliminate all duplicates before proceeding.",
               "The duplicate records include:", paste(head(dupes2$UniqueID, 15), collapse = ", ")), call. = FALSE)
  }
  rm(Uniq)
  
  ########################################################################.
  ###                          Check for locations not in database    ####
  ########################################################################.
  
  # Bring in locations table
  db_locations <- na.omit(dbGetQuery(pool, glue("SELECT [LocationMWRA] FROM [{schema}].[tblLocations]")))
  new_locs <- setdiff(
    df.wq %>%
      filter(!Site %in% c("FIELD_QC_DUP", "MISC")) %>% .$Site,
    db_locations$LocationMWRA
  )
  if (length(new_locs) > 0) {
    stop(paste0(
      "The following locations are in the data but not in tblLocations: ",
      paste0(new_locs, collapse = ", "),
      ". Fix data site names or update tblLocations before importing."
    ))
  }
  
  ###############################################################################################
  
  ###############################################################################################
  
  ### DataSource
  df.wq <- df.wq %>% mutate(DataSource = file)
  
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
    query.wq <- dbGetQuery(pool, glue("SELECT max(ID) FROM [{schema}].[{ImportTable}]"))
    # Get current max ID
    if(is.na(query.wq)) {
      query.wq <- 0
    } else {
      query.wq <- query.wq
    }
    ID.max.wq <- as.numeric(unlist(query.wq))
    rm(query.wq)
    
    ### ID wq
    df.wq$ID <- seq.int(nrow(df.wq)) + ID.max.wq }
  
  df.wq$ID <- setIDs()
  
  
  # Reorder remaining columns to match the database table exactly ####
  col.order.wq <- dbListFields(pool, schema_name = schema, name = ImportTable)
  df.wq <- df.wq[, col.order.wq]
  
  ### QC Test ####
  source("src/Functions/WITQCTEST.R", local = T)
  
  # QCCHECK needs the sampling location column to be called "Location" and not "Site"
  df.wq.loc <- df.wq %>% mutate(Location = Site)
  
  qc_message <- QCCHECK( df.qccheck = df.wq.loc, 
                         file = file, 
                         ImportTable = ImportTable)
  print(qc_message)

  # Create a list of the processed datasets
  dfs <- list()
  dfs[[1]] <- df.wq
  dfs[[2]] <- path
  dfs[[3]] <- NULL # Removed condition to test for flags and put it in the setFlagIDS() function
  
  # Disconnect from db and remove connection obj
  poolClose(pool)
  rm(pool)
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
  schema <- 'Quabbin'
  tz <- 'America/New_York'
  pool <- dbPool(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)
  
  poolWithTransaction(pool, function(conn) {
    pool::dbWriteTable(pool, DBI::Id(schema = schema, table = ImportTable),value = df.wq, append = TRUE, row.names = FALSE)
  })
  
  #* Close the database pool ----
  poolClose(pool)
  rm(pool)
  
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
