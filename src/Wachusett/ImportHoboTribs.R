##############################################################################################################################
#     Title: ImportHoboTribs.R
#     Description: This script will process raw HOBO data files containing stage and
#     temperature data by calculating discharge using rating coefficients from the Wachusett Hydro Database (tblRatings).
#     Output: df.wq - Daily flow/water temp records imported to WQDB
#     Written by: Dan Crocker, July 2017, revised November 2017
##############################################################################################################################

# LOAD REQUIRED LIBRARIES
# library(RODBC)
# library(odbc)
# library(DBI)
# library(tidyverse)
# library(data.table)
# library(lubridate)
# library(DescTools)
# library(scales)
# library(readxl) # new to swap code that gets the data to read_excel()

# START PROCESSING THE DATA #
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable){
  # Get the full path to the file
   path <- paste0(rawdatafolder, "/", file)
   # Read in the data to a dataframe
   df.wq <- read_excel(path, sheet = 1, col_names = TRUE, trim_ws = TRUE, range = cell_cols("A:F")) # This is the raw stage data

  # Remove first row which contains useless column headers and only keep columns 1,3,5
  df.wq <- df.wq[-1,c(2,4,6)]

  # Change column names
  names(df.wq) = c("DateTime", "WaterTemp_C", "Stage_ft")
  df.wq <- df.wq[!df.wq$DateTime == "",]

  # Change data types to numeric
  df.wq[sapply(df.wq, is.character)] <- lapply(df.wq[sapply(df.wq, is.character)], as.numeric)

  # Convert DateTime format and update timezone
  df.wq$DateTime <- XLDateToPOSIXct(df.wq$DateTime)
  df.wq$DateTime <- force_tz(df.wq$DateTime, tzone = "America/New_York")
  
  # Filter out blank times due to daylight savings time 
  df.wq <- filter(df.wq, !is.na(DateTime))
  
  # Convert F to C
  df.wq$WaterTemp_C <- round(5/9 * (df.wq$WaterTemp_C -32), 2)

###########################################
# GET RATING INFORMATION FROM WQ DATABASE #
###########################################

# Set odbc connection  and get the rating table
  con <-  odbcConnectAccess(filename.db)
  ratings <- sqlFetch(con, "tblRatings")
# Assigntoday's date as the end date for ratings that are still valid - so that date test won't compare against NA values
  now <- format(Sys.time(), "%Y-%m-%d")
  ratings$End[is.na(ratings$End)] <- now
# Get the tributary code for this stage data
  trib <- substr(file,1,4)
# Extract ratings for current trib
  ratings2 <- ratings[ratings$MWRA_Loc == trib,]

################### MAKE COPY OF DATA TO USE GOING FORWARD
 # df.wq <- sd
###################

# Add three new columns to hold ratingNo, rating part, and calculated discharge
  df.wq$ratingNo <- 0
  df.wq$part <- 0
  df.wq$q_cfs <- 0

######################################
# BEGIN LOOPING THROUGH STAGE VALUES #
######################################

# For each stage value assign the appropriate rating number based on the date ranges of the ratings
# x is the each date value

  x <- df.wq$DateTime
  df.wq$ratingNo <- sapply(x, function(x) ratings2$ID[ratings2$Start <= x & ratings2$End >= x])
  df.wq$ratingNo <-  as.numeric(df.wq$ratingNo)


#################################################################
# Assign the rating part to each stage
x <- df.wq$ratingNo
y <- df.wq$Stage_ft

part <- function(x,y) {
if(ratings2$Parts[ratings2$ID == x] == 1) {# Rating has 1 part
  1
  } else if(ratings2$Parts[ratings2$ID == x] == 2) { # Rating has 2 parts
      if(y < ratings2$Break1[ratings2$ID == x]) {# stage is less than breakpoint 1
      1
    } else 2 # Otherwise stage is >= breakpoint1
    # If no return yet, then the rating has 3 parts
  } else {
    if(y[df.wq$ratingNo == x] < ratings2$Break1[ratings2$ID == x]) { # stage is less than breakpoint 1
      1 # The stage is in the first part of the rating
      } else if(y[df.wq$ratingNo == x] >= ratings2$Break2[ratings2$ID == x]) { # stage is higher than breakpoint 2
          3
        } else 2
  }
}

df.wq$part <- mapply(part,x,y) %>% as.numeric()

# Create a list to hold records for Below Rating Curve Values
# Get number of records in dataset:
Nrecs <- as.numeric(length(df.wq$DateTime))
BRC_Flags <- vector("list", Nrecs)
# Create a list to hold records for Above Rating Curve Values
ARC_Flags <- vector("list", Nrecs)
j <- 1 # Start the BRC list value at one
k <- 1 # Start the ARClist value at one

# Define function to find Q:
findq <- function(stage, C, n, a) {
  C*(stage-a)^n
}
# LOOP THROUGH ALL STAGE VALUES AND CALCULATE THE DISCHARGE USING RATING COEFFICIENTS
  for (i in seq_along(df.wq$q_cfs)) {
    # Get the min and max stage bounds for the current rating

    minstage <- ratings2$MinStage[ratings2$ID == df.wq$ratingNo[i]]
    maxstage <- ratings2$MaxStage[ratings2$ID == df.wq$ratingNo[i]]

    if(df.wq$Stage_ft[i] < minstage) { # Stage is below the rating curve (PZF) assign flow of zero and move to next record
      df.wq$q_cfs[i] <- 0
      BRC_Flags[[j]] <- as.Date(df.wq$DateTime)[i] # Have to flag the entire date, since output is daily, *Not all records on date may be affected
      j <- j + 1
      next
      } else {
        if(df.wq$Stage_ft[i] > maxstage) { # Stage is above the rating curve and cannot be calculated -
          # Set the stage to the max stage and add ARC_Flags
          df.wq$Stage_ft[i] <- maxstage
          ARC_Flags[[k]] <- as.Date(df.wq$DateTime)[i] # Have to flag the entire date, since output is daily, *Not all records on date may be affected
          k <- k + 1 # Change the list slot to the next one
        }
          # Rating Coefficients part 1
          c1 <- ratings2$C1[ratings2$ID == df.wq$ratingNo[i]]
          a1 <- ratings2$a1[ratings2$ID == df.wq$ratingNo[i]]
          n1 <- ratings2$n1[ratings2$ID == df.wq$ratingNo[i]]
          # Rating Coefficients part 2
          c2 <- ratings2$C2[ratings2$ID == df.wq$ratingNo[i]]
          a2 <- ratings2$a2[ratings2$ID == df.wq$ratingNo[i]]
          n2 <- ratings2$n2[ratings2$ID == df.wq$ratingNo[i]]
          # Rating Coefficients part 3
          c3 <- ratings2$C3[ratings2$ID == df.wq$ratingNo[i]]
          a3 <- ratings2$a3[ratings2$ID == df.wq$ratingNo[i]]
          n3 <- ratings2$n3[ratings2$ID == df.wq$ratingNo[i]]

          C <- paste0("c", df.wq$part[i])
          a <- paste0("a", df.wq$part[i])
          n <- paste0("n", df.wq$part[i])
        }
    # Use findq function to calculate discharge from each stage
      df.wq$q_cfs[i] <- findq(stage = df.wq$Stage_ft[i], C = get(C), a = get(a), n = get(n))
  }

# Remove NULL values from BRC and ARC lists
BRC_Flags[sapply(BRC_Flags, is.null)] <- NULL
ARC_Flags[sapply(ARC_Flags, is.null)] <- NULL

# Convert to dataframe, convert dates back to date format,
if(length(BRC_Flags) > 0){
BRC_Flags <- data.frame(as.Date(unique(unlist(BRC_Flags)), origin = "1970-01-01"))
names(BRC_Flags) <- "DAY"
BRC_Flags$FlagCode <- 113
} else {
  BRC_Flags <- NULL
}
if(length(ARC_Flags) > 0){
ARC_Flags <- data.frame(as.Date(unique(unlist(ARC_Flags)), origin = "1970-01-01"))
names(ARC_Flags) <- "DAY"
ARC_Flags$FlagCode <- 111
} else {
  ARC_Flags <- NULL
}

# # Create new dataframe that calculates daily min, mean, max
  df.wq <- df.wq %>%
    group_by(date(df.wq$DateTime)) %>%
    summarize(stage_min = min(Stage_ft), stage_mean = mean(Stage_ft), stage_max = max(Stage_ft),
              q_min_cfs = min(q_cfs), q_mean_cfs = mean(q_cfs), q_max_cfs = max(q_cfs),
              WaterTemp_min = min(WaterTemp_C), WaterTemp_mean = mean(WaterTemp_C), WaterTemp_max = max(WaterTemp_C))

# Change first column name to "Date"
  colnames(df.wq)[1] <- "DATE"
# Assign the appropriate trib code
  df.wq$TRIBUTARY <- trib
# Generate a new ID number for the rating
  hobo <- sqlFetch(con, "tblHOBO_DATA")
  LastID <- as.numeric(max(hobo$ID))
  if(LastID == -Inf) {
    LastID <- 0
  } else {
    LastID <- LastID
  }
  df.wq$ID <- seq.int(nrow(df.wq)) + LastID

  # Make sure all column headers are uppercase and then rearrange columns and set data type to data frame
  names(df.wq) <- toupper(names(df.wq))
  df.wq <-  df.wq[, c(12,11, 1:10)]
  df.wq <- as.data.frame(df.wq)

  #Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)

  # Connect to db using odbc for queries below:
  con <- dbConnect(odbc::odbc(),
                   .connection_string = paste("driver={Microsoft Access Driver (*.mdb, *.accdb)}",
                                              paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                   timezone = "America/New_York")

  # Make the df for any flags
    setFlagIDs <- function(){
      if(!is.null(ARC_Flags) | !is.null(BRC_Flags)){ # Condition returns TRUE at least one is not null
        query.flags <- dbGetQuery(con,"SELECT max(ID) FROM tblHOBO_FlagIndex")
        # Get current max ID
        if(is.na(query.flags)) {
          query.flags <- 0
        } else {
          query.flags <- query.flags
        }
        ID.max.flags <- as.numeric(unlist(query.flags))
        rm(query.flags)

        # Bind rows of flag dfs
        df.flags <- bind_rows(BRC_Flags,ARC_Flags)
        # Match the Sample ID number using the date
        df.flags$SampleID <- df.wq$ID[match(df.flags$DAY,df.wq$DATE)]

        ### ID flags
        df.flags$ID <- seq.int(nrow(df.flags)) + ID.max.flags
        df.flags$DateFlagged = as.Date(Sys.time())
        df.flags$ImportStaff = Sys.getenv("USERNAME")

        # Reorder df.flags columns to match the database table exactly- the Date column is dropped
        df.flags <- df.flags[,c(4,3,2,5,6)]
      } else { # Condition False - There were no ARC or BRC stages
        df.flags <- NA
      } # End flags processing chunk
    } # End set flags function
    df.flags <- setFlagIDs()

  # Create a list of the processed datasets
  dfs <- list()
  dfs[[1]] <- df.wq
  dfs[[2]] <- path
  dfs[[3]] <- df.flags

  return(dfs)
 } # PROCESS_DATA - STOP HERE AND INSPECT DATA BEFORE IMPORTING TO WQ DATABASE

# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db) # Run the function to process the stage/water temp data
#
# INSPECT DATA TO MAKE SURE IT LOOKS GOOD BEFORE IMPORTING TO WQDB
#

##############
#IMPORT DATA #
##############
 IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder,ImportTable, ImportFlagTable = NULL){

# processedfolder <- "W:/WatershedJAH/EQStaff/WQDatabase/TribStages_HOBOs/Processed_HOBO_data"

# Connect to db using RODBC
   con <-  odbcConnectAccess(filename.db)
# Save the discharge and temp data to the table in the WQDatabase
  sqlSave(con, df.wq, tablename = "tblHOBO_DATA", append = TRUE,
          rownames = FALSE, colnames = FALSE, addPK = FALSE , fast = F)

# Save the Flags to the flag index table
  if (!is.na(df.flags)){ # Check and make sure there is flag data to import
    sqlSave(con, df.flags, tablename = "tblHOBO_FlagIndex", append = T,
            rownames = F, colnames = F, addPK = F , fast = F)
  }

  # Move the processed hobo file to the processed folder
  file.rename(path, paste0(processedfolder,"/",file))
#Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)
 }
