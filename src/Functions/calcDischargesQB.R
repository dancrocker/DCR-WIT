##############################################################################################################################
#     Title: calcDischargesQB.R
#     Description: This script is called from an import function with stage values - it uses rating information to calculate
#                   discharges for each stage record
#     Written by: Dan Crocker
#     Last Update: February2019
#
##############################################################################################################################

# Load libraries needed
  library(tidyverse)
  library(odbc)
  library(RODBC)
  library(DBI)
  library(lubridate)
  library(magrittr)
  # library(dataRetrieval)


calcQ <- function(filename.db, stages) {
# Set odbc connection  and get the rating table
con <-  odbcConnectAccess(filename.db)
ratings <- sqlFetch(con, "tblRatings")

# Disconnect from db and remove connection obj
RODBC::odbcCloseAll()
rm(con)
# Assigntoday's date as the end date for ratings that are still valid - so that date test won't compare against NA values
now <- format(Sys.time(), "%Y-%m-%d")
ratings$End[is.na(ratings$End)] <- now
# Pull stage records that need to get converted to discharge - use this df at the end of script to merge Q records with df_wq
# stages <- ToCalc
stages <- stages %>%
  mutate(UniqueID = str_replace(stages$UniqueID,pattern = "_HT","_QCFS"))
HOBOcalc <- stages[stages$Location %in% c("MD61","EBU"),] 
# HOBOcalc <- stages[!stages$Location %in% c("MD04","MD07","MD69"),] # NOT THE USGS GAUGES
# USGScalc <- stages[stages$Location %in% c("MD04","MD07","MD69"),]
count <- 0
if(nrow(HOBOcalc) > 0){
  count <- 1
    # Create a working df for HOBO discharges
    df_Q <- select(HOBOcalc, c("Location","ResultReported","SampleDateTime","UniqueID")) %>%
      dplyr::rename( "Stage_ft" = ResultReported) %>%
      mutate(ratingNo = 0, part = 0, q_cfs = 0)
    
    df_Q$Stage_ft <- as.numeric(df_Q$Stage_ft)
    
    # Reduce rating table to locations within dataset and remove ratings with unspecified start times
    ratings2 <- ratings[ratings$MWRA_Loc %in% df_Q$Location & !is.na(ratings$Start),]
    
    # For each stage value assign the appropriate rating number based on the date ranges of the ratings
    
    x <- df_Q$SampleDateTime
    y <- df_Q$Location
    
    pickRating <- function(x,y){
      ratingNo <- ratings2$ID[ratings2$Start <= x & ratings2$End >= x & ratings2$MWRA_Loc == y]
    }
    
    df_Q$ratingNo <- map2(x, y, pickRating) %>%
      as.numeric()
    
    #################################################################
    # Assign the rating part to each stage
    x <- df_Q$ratingNo
    y <- df_Q$Stage_ft
    
    part <- function(x,y) {
      if(ratings2$Parts[ratings2$ID == x] == 1) {# Rating has 1 part
        1
      } else if(ratings2$Parts[ratings2$ID == x] == 2) { # Rating has 2 parts
        if(y < ratings2$Break1[ratings2$ID == x]) {# stage is less than breakpoint 1
          1
        } else 2 # Otherwise stage is >= breakpoint1
        # If no return yet, then the rating has 3 parts
      } else {
        if(y[df_Q$ratingNo == x] < ratings2$Break1[ratings2$ID == x]) { # stage is less than breakpoint 1
          1 # The stage is in the first part of the rating
        } else if(y[df_Q$ratingNo == x] >= ratings2$Break2[ratings2$ID == x]) { # stage is higher than breakpoint 2
          3
        } else 2
      }
    }
    
    df_Q$part <- mapply(part,x,y) %>% as.numeric()
    
    # Create a list to hold records for Below Rating Curve Values
    # Get number of records in dataset:
    Nrecs <- as.numeric(length(df_Q$SampleDateTime))
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
    for (i in seq_along(df_Q$q_cfs)) {
      # Get the min and max stage bounds for the current rating
    
      minstage <- ratings2$MinStage[ratings2$ID == df_Q$ratingNo[i]]
      maxstage <- ratings2$MaxStage[ratings2$ID == df_Q$ratingNo[i]]
    
      if(df_Q$Stage_ft[i] < minstage) { # Stage is below the rating curve (PZF) assign flow of zero and move to next record
        df_Q$q_cfs[i] <- 0
        BRC_Flags[[j]] <- df_Q$UniqueID[i] #
        j <- j + 1
        next
      } else {
        if(df_Q$Stage_ft[i] > maxstage) { # Stage is above the rating curve and cannot be calculated -
          # Set the stage to the max stage and add ARC_Flags
          df_Q$Stage_ft[i] <- maxstage
          ARC_Flags[[k]] <- df_Q$UniqueID[i] # Have to flag the entire date, since output is daily, *Not all records on date may be affected
          k <- k + 1 # Change the list slot to the next one
        }
        # Rating Coefficients part 1
        c1 <- ratings2$C1[ratings2$ID == df_Q$ratingNo[i]]
        a1 <- ratings2$a1[ratings2$ID == df_Q$ratingNo[i]]
        n1 <- ratings2$n1[ratings2$ID == df_Q$ratingNo[i]]
        # Rating Coefficients part 2
        c2 <- ratings2$C2[ratings2$ID == df_Q$ratingNo[i]]
        a2 <- ratings2$a2[ratings2$ID == df_Q$ratingNo[i]]
        n2 <- ratings2$n2[ratings2$ID == df_Q$ratingNo[i]]
        # Rating Coefficients part 3
        c3 <- ratings2$C3[ratings2$ID == df_Q$ratingNo[i]]
        a3 <- ratings2$a3[ratings2$ID == df_Q$ratingNo[i]]
        n3 <- ratings2$n3[ratings2$ID == df_Q$ratingNo[i]]
    
        C <- paste0("c", df_Q$part[i])
        a <- paste0("a", df_Q$part[i])
        n <- paste0("n", df_Q$part[i])
      }
      
    if(df_Q$part[i] > 2){ ### Then the v-notch is full and the rectangular portion of the equation must be used. 
       vnotchQ <- findq(stage = df_Q$Stage_ft[i], C = get(C), a = get(a), n = get(n))
      ### Calculate the head of water in the rectangular portion of the weir
       H2 <- df_Q$Stage_ft[i] - 2
       ### Calculate the discharge in the rectangular portion of the weir 
      rectQ <-  c2 * a2 * H2^n2 
      df_Q$q_cfs[i] <- vnotchQ + rectQ
      } else {
      # Use findq function to calculate discharge from each stage
      df_Q$q_cfs[i] <- findq(stage = df_Q$Stage_ft[i], C = get(C), a = get(a), n = get(n))
      }
    
    df_Q$q_cfs <- round(df_Q$q_cfs, digits = 2)
    # Remove NULL values from BRC and ARC lists
    BRC_Flags[sapply(BRC_Flags, is.null)] <- NULL
    ARC_Flags[sapply(ARC_Flags, is.null)] <- NULL
    
    # Convert to dataframe - This is just a list of SampleDateTimes
    if(length(BRC_Flags) > 0){
      BRC_Flags <- data.frame(unlist(BRC_Flags))
      names(BRC_Flags) <- "UNQID"
      BRC_Flags$FlagCode <- 113
    } else {
      BRC_Flags <- NULL
    }
    if(length(ARC_Flags) > 0){
      ARC_Flags <- data.frame(unlist(ARC_Flags))
      names(ARC_Flags) <- "UNQID"
      ARC_Flags$FlagCode <- 111
    } else {
      ARC_Flags <- NULL
    }
    if(length(ARC_Flags) + length(BRC_Flags) > 0){
        df_QFlags <- bind_rows(BRC_Flags,ARC_Flags)
    } else {
      df_QFlags <- NA
    }

df_HOBO <- HOBOcalc %>%
  mutate(Analysis = "CALCULATED",
         ReportedName = NA,
         Parameter = "Discharge",
         Units = "cfs",
         Status = NA,
         Reportable = NA,
         ResultReported = as.character(df_Q$q_cfs)
         )
    }
} ### End HOBO discharge Calculations
### Calculate USGS discharges --- Does not apply to Quabbin
# if(nrow(USGScalc) > 0){
# count <- count + 2
# # Create a working df
# qusgs <- select(USGScalc, c(4,27,28)) %>%
#   mutate(Discharge = 0)
# 
# # Filter each location, and for each record, run the data retrieval query
# 
# locs <- c("MD04", "MD69", "MD07")
# pCodes <- "00060" # 60 is discharge
# 
# for (j in seq_along(qusgs$SampleDateTime)) {
# 
#   # Assign siteNo based Location, then assign the start date of data
#   if (grepl("MD04",qusgs$Location[j])) {
#     siteNo <- "01095434"   #GATES BROOK WEST BOYLSTON, MA
#     start <- as.POSIXct("2011-12-14")
#   } else {
#     if (grepl("MD69",qusgs$Location[j])) {
#       siteNo <- "01095375"   # 01095375	 QUINAPOXET RIVER AT CANADA MILLS NEAR HOLDEN, MA
#       start <- as.POSIXct("1996-11-21")
#     } else {
#       siteNo <- "01095220"   # 01095220	 STILLWATER RIVER NEAR STERLING, MA
#       start <- as.POSIXct("1994-04-21")
#     }
#   } # End if
# 
#   # Define the date of the stage timestamp to lookup (Can't use the listed stage value because of possible shifts)
#   flowdate <- as.character(date(qusgs$SampleDateTime[j]))
# 
#   # Test if the flowdate is less than the start date
#   if(date(qusgs$SampleDateTime[j]) < start){
#     # There can be no discharge, move to the next one
#     next
#   } else { # Run the query to match the
#     hg <- readNWISuv(siteNumbers = siteNo,
#                      parameterCd = pCodes,
#                      startDate = flowdate,
#                      endDate = flowdate,
#                      tz="America/New_York") %>%
#       renameNWISColumns() # Convenience function to rename columns
#   }
# 
#   # Test it the query returned any results, if so, move on, if not, move to the next one
#   if(length(hg$dateTime) < 1) {
#     qusgs$Discharge[j] <- NA
#     print(paste(qusgs$Location[j],qusgs$SampleDateTime[j]," -No discharge data available for period selected"))
#     next #There was no discharge data available for this day
#   } else { # There is discharge data available - lookup dischage associated with the time of the sample
#     time <-  qusgs$SampleDateTime[j]
#     if (siteNo == "01095434"){
#       flow <- hg$Flow_Inst[hg$dateTime == round_date(time, "10 mins")]
#     } else {
#       flow <- hg$Flow_Inst[hg$dateTime == round_date(time, "15 mins")]
#     }
#     if(length(flow)>0){
#       qusgs$Discharge[j] <- flow
#     } else {
#       print(paste(qusgs$Location[j],qusgs$SampleDateTime[j],"There is no matching flow for the specified time"))
#       next
#     }
#   }
# } #End Loop
# qusgs$Discharge <- as.numeric(qusgs$Discharge)
# 
# df_USGS <- USGScalc %>%
#   mutate(Analysis = "NWIS LOOKUP",
#          ReportedName = NA,
#          Parameter = "Discharge",
#          Units = "cfs",
#          Status = NA,
#          Reportable = NA,
#          ResultReported = as.character(qusgs$Discharge)
#   )
# } # End USGS Calcs

# Combine HOBO and USGS Discharges depending on count value:
# 0 -- There are no discharges - this should not happen since this script is triggered only when there are possible discharges to calculate
# 1 -- There are HOBO discharges, but no USGS
# 2 -- There are USGS discharges, but no HOBO
# 3 -- THere are both USGS and HOBO Discharges 

# if(count == 3){
#   df_Q <- bind_rows(df_HOBO, df_USGS)
#   } else {
#     if(count == 1){
#       df_Q <- df_HOBO
#     } else {
#       df_Q <- df_USGS
#     }
# }

Q_dfs <- list(df_Q = df_HOBO,
              df_QFlags = df_QFlags)
return(Q_dfs)
} # End function

##############################################################################################
# Q_dfs <- calcQ(filename.db = filename.db, stages = ToCalc)
