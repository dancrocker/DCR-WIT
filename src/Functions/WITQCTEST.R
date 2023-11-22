
################################### HEADER ###################################
#  TITLE: WIT QC Test
#  DESCRIPTION: Compare imported data to historical ranges and percentiles. If outliers found, produces QC log and outputs text for email.
#  AUTHOR(S): Travis Drury
#  DATE LAST UPDATED: 12/17/19
#  GIT REPO:
#  R version 3.6.0 (2019-04-26)  i386
##############################################################################

###  Load required dplyr package
# library(dplyr)

#Create function for evaluating bacteria RPD
BACT_DUP_TEST <- function(TribResult, DupResult, RPD){
  if_else((abs(TribResult - DupResult) <= 50), "PASS",
          case_when((TribResult > 5000 | DupResult > 5000) & RPD >= 5 ~ "FAIL",
                    (TribResult > 500 | DupResult > 500) & RPD >= 10 ~ "FAIL",
                    (TribResult > 50 | DupResult > 50) & RPD >= 20 ~ "FAIL",
                    RPD >= 30 ~ "FAIL",
                    TRUE ~ "PASS"))
}

### Function compares df.qccheck records (in the format at the end of WIT processing before importing) to historical min/max and percentiles
QCCHECK <- function(df.qccheck, file, ImportTable){

### FETCH CACHED DATA FROM WAVE RDS FILES ####
  
  files <-  c("trib_wach_summary.rds")
  
  datadir <- paste0(user_root, config[["DataCache"]])
  rds_files <- list.files(datadir, full.names = F , pattern = ".rds")
  rds_in <- which(rds_files %in% files) %>% as.numeric()
  rds_files <- rds_files[rds_in]
  data <- lapply(paste0(datadir,"/", files), readRDS)
  df_names <- gsub('.rds', '', list.files(datadir, pattern = '.rds'))
  df_names <- df_names[rds_in]
  names(data) <- df_names
  list2env(data ,.GlobalEnv)
  rm(data)


### Get table to match up QC dups/blanks  
  
  ### Connect to Database ####  
  dsn <- "DCR_DWSP_App_R"
  database <- "DCR_DWSP"
  schema <- 'Wachusett'
  tz <- 'UTC'
  con <- dbConnect(odbc::odbc(), dsn = dsn, uid = dsn, pwd = config[["DB Connection PW"]], timezone = tz)
  
  # Get dataframe for duplicate/blank locations
  dup_df <- dbReadTable(con, Id(schema = schema, table = "tbl_Field_QC"))
  df_wach_param <- dbReadTable(con, Id(schema = schema, table = "tblParameters"))
  dbDisconnect(con)
  rm(con)
  
### Drop Edit_timestamp from tblParameters
  df_wach_param <- df_wach_param %>%
    select(-Edit_timestamp)
  
### Create empty dataframes for QC results with output column names
statoutliers <- df.qccheck[NULL,names(df.qccheck)]
statoutliers<-dplyr::mutate(statoutliers, HistoricalMin=NA, Percentile25=NA, HistoricalMedian=NA, Percentile75=NA, HistoricalMax=NA, IQR=NA)

rangeoutliers <- df.qccheck[NULL,names(df.qccheck)]
rangeoutliers<-dplyr::mutate(rangeoutliers, HistoricalMin=NA, HistoricalMean=NA, HistoricalMax=NA)

### Creates input dataframe without Staff Gauge Height for QC check against trib_wach_summary
df.qccheckNOgauge <- dplyr::filter(df.qccheck,Parameter!="Staff Gauge Height")

### Loop to look for correct parameter/units, compare results with possible min/max, and then QC check against trib_wach_summary

for (i in 1:nrow(df.qccheck)){

  # Stop if Parameter and Units are not in tblParameters  
    if (!df.qccheck$Parameter[i] %in% dplyr::filter(df_wach_param,ParameterName==df.qccheck$Parameter[i],ParameterUnits==df.qccheck$Units[i])$ParameterName &
      !df.qccheck$Parameter[i] %in% dplyr::filter(df_wach_param,ParameterName==df.qccheck$Parameter[i],ParameterUnits==df.qccheck$Units[i])$ParameterUnits){
      print(paste0("Parameter ",df.qccheck$Parameter[i]," with units ",df.qccheck$Units[i]," not found in tblParameters. Fix and process again."))
      stop(paste0("Parameter ",df.qccheck$Parameter[i]," with units ",df.qccheck$Units[i]," not found in tblParameters. Fix and process again."))
  
  } else {  
  
  # Stop if FinalValue is lower than possible low value    
  if (df.qccheck$FinalResult[i] < dplyr::filter(df_wach_param,ParameterName==df.qccheck$Parameter[i],ParameterUnits==df.qccheck$Units[i])$MinPossible){  
    print(paste0(df.qccheck$UniqueID[i]," value of ",df.qccheck$FinalResult[i]," is lower than possible for ",df.qccheck$Parameter[i],". Fix and process again."))
    stop(paste0(df.qccheck$UniqueID[i]," value of ",df.qccheck$FinalResult[i]," is lower than possible for ",df.qccheck$Parameter[i],". Fix and process again."))

  } else {
    
  # Stop if FinalValue is higher than possible high value     
  if (df.qccheck$FinalResult[i] > dplyr::filter(df_wach_param,ParameterName==df.qccheck$Parameter[i],ParameterUnits==df.qccheck$Units[i])$MaxPossible){  
    print(paste0(df.qccheck$UniqueID[i]," value of ",df.qccheck$FinalResult[i]," is higher than possible for ",df.qccheck$Parameter[i],". Fix and process again."))
    stop(paste0(df.qccheck$UniqueID[i]," value of ",df.qccheck$FinalResult[i]," is higher than possible for ",df.qccheck$Parameter[i],". Fix and process again."))

  } else {    
  
    ### QC check against trib_wach_summary
    
    # Only runs QC check on each result if the location/parameter/unit combination in the imported data is present in the summary RDS file, else will skip that record and proceed to next record
  if (df.qccheckNOgauge$Location[i] %in% dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$Site
        & df.qccheckNOgauge$Parameter[i] %in% dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$Parameter
        & df.qccheckNOgauge$Units[i] %in% dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$Units){
      
    # Add to rangeoutliers if less than historical minimum for that site/parameter    
  if (df.qccheckNOgauge$FinalResult[i] < dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$Min){
    rangeoutliers <- df.qccheckNOgauge[i,] %>%
      cbind (trib_wach_summary %>%
               filter (Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i]) %>%
               select (one_of(c("Min","Mean","Max"))) %>%
               rename (HistoricalMin = Min, 
                       HistoricalMean = Mean, 
                       HistoricalMax = Max)) %>%
              bind_rows (rangeoutliers)
  } else{

    # Add to rangeoutliers if greater than historical maximum for that site/parameter            
    if (df.qccheckNOgauge$FinalResult[i] > dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$Max){
      rangeoutliers <- df.qccheckNOgauge[i,] %>%
        cbind (trib_wach_summary %>%
                 filter (Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i]) %>%
                 select (one_of(c("Min","Mean","Max"))) %>%
                 rename (HistoricalMin = Min, 
                         HistoricalMean = Mean, 
                         HistoricalMax = Max)) %>%
                bind_rows (rangeoutliers)
    } else{

      # Add to statoutliers if FinalValue is less than (25th percentile - 1.5 times the interquartile range) that site/parameter 
      if (df.qccheckNOgauge$FinalResult[i] < ((dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$percentile25)-(1.5*(dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$IQR)))){
        statoutliers <- df.qccheckNOgauge[i,] %>%
          cbind (trib_wach_summary %>%
                   filter (Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i]) %>%
                   select (one_of(c("Min","percentile25","Median","percentile75","Max","IQR"))) %>%
                   rename (HistoricalMin = Min,
                           Percentile25 = percentile25, 
                           HistoricalMedian = Median, 
                           Percentile75 = percentile75,
                           HistoricalMax = Max,
                           IQR = IQR)) %>%
                  bind_rows (statoutliers)
      } else{

        # Add to statoutliers if FinalValue is greater than (75th percentile + 1.5 times the interquartile range) that site/parameter        
        if (df.qccheckNOgauge$FinalResult[i] > ((dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$percentile75)+(1.5*(dplyr::filter(trib_wach_summary,Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i])$IQR)))){
          statoutliers <- df.qccheckNOgauge[i,] %>%
                    cbind (trib_wach_summary %>%
                     filter (Site==df.qccheckNOgauge$Location[i],Parameter==df.qccheckNOgauge$Parameter[i],Units==df.qccheckNOgauge$Units[i]) %>%
                     select (one_of(c("Min","percentile25","Median","percentile75","Max","IQR"))) %>%
                       rename (HistoricalMin = Min,
                               Percentile25 = percentile25, 
                               HistoricalMedian = Median, 
                               Percentile75 = percentile75,
                               HistoricalMax = Max,
                               IQR = IQR)) %>%
                    bind_rows (statoutliers)
        }
      }}}}}}}}


### Checking duplicates and blanks
dups <- df.qccheckNOgauge %>% filter(Location %in% c("WFD1","WFD2","WFD3")) %>%
  rename(Duplicate = Location) %>%
  mutate(Date = as.Date(DateTimeET))

# Only proceed if there are duplicates in the data
if(nrow(dups)>0){

# Rename column for joining  
dup_df_rename <- dup_df %>% rename(Duplicate = Dup_Blank_code)

# Join duplicates with locations
dups <- inner_join(dups, dup_df_rename, by=c("Date","Duplicate")) %>% 
  rename(Location = MWRA_Location) %>%
  select(Date, Duplicate, Location, Parameter, Units, FinalResult)

# Add date to original dataset
df.qccheckNOgauge.date <- df.qccheckNOgauge %>% mutate(Date = as.Date(DateTimeET))

# Combine duplicate and trib results
dups_combined <- inner_join(dups, df.qccheckNOgauge.date, by=c("Date","Location","Parameter","Units")) %>%
  rename(TribResult = "FinalResult.y",
         DupResult = "FinalResult.x")

### Calculating RPD for bacteria dups

# Analyze bacteria dups
bact_dups <- dups_combined %>% 
  filter(Parameter == "E. coli") %>%
  mutate(Log10DupResult = log10(DupResult),
         Log10TribResult = log10(TribResult),
         RPD = round(((abs(Log10TribResult-Log10DupResult)/((Log10TribResult+Log10DupResult)/2))*100),digits=1),
         Pass = BACT_DUP_TEST(TribResult, DupResult, RPD))

# Analyze any DO dups
DO_temp_dups <- dups_combined %>% 
  filter(Parameter %in% c("Dissolved Oxygen", "Water Temperature")) %>%
  mutate(RPD=abs(TribResult-DupResult),
         Pass = if_else(RPD>0.2, "FAIL","PASS"))

# Analyze all other dups
dups_other <- dups_combined %>% 
  filter(!Parameter %in% c("E. coli", "Dissolved Oxygen", "Water Temperature","Oxygen Saturation")) %>%
  mutate(RPD = round(((abs(TribResult-DupResult)/((TribResult+DupResult)/2))*100),digits=1),
         Pass = if_else(RPD>30, "FAIL","PASS"))

#Combine datasets
dups_all <- bind_rows(bact_dups, DO_temp_dups, dups_other)

#Get failed dups
dups_fail <- filter(dups_all, Pass == "FAIL") %>%
                rename(DupCode = Duplicate,
                       SampleResult = TribResult,
                       DuplicateResult = DupResult)
}else{
  #If no duplicates in data, create an empty dataframe for failed duplicates
  dups_fail<-as.data.frame(NULL)
}

### Blanks

#Get blanks
blanks <- df.qccheckNOgauge %>% filter(Location %in% c("WFB1","WFB2")) %>%
  rename(Blank = Location) %>%
  mutate(Date = as.Date(DateTimeET))

#Only proceed if there are blanks in the data
if(nrow(blanks)>0){

#Rename for joining  
blank_df_rename <- dup_df %>% rename(Blank = Dup_Blank_code)

#Join blanks with locations
blanks <- inner_join(blanks, blank_df_rename, by=c("Date","Blank")) %>% 
  rename(Location = MWRA_Location) 

#Calculate whether blanks fail
blanks <- blanks %>% 
  # rowwise %>% 
  mutate(
    Pass = case_when(
      (Parameter =="Turbidity NTU" & FinalResult >=1) ~ "FAIL",
      (str_detect(ResultReported,"<")==FALSE) ~ "FAIL",
      TRUE ~ "PASS")
  )
# Get only failed blanks
blanks_fail <- filter(blanks, Pass == "FAIL") %>%
  dplyr::rename(ID = ID.x,
                BlankCode = Blank)
}else{
  #If there are no blanks, make empty dataframe for failed blanks
  blanks_fail <- as.data.frame(NULL)}

### Print results of outlier check to unique WIT log
QC_log_dir <- paste0(wach_team_root, config[["QC_Logfiles"]])
  # Delete a log of the same name if it exists
  if (file.exists(paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))){
    file.remove(paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
  }

### If any failed duplicates, add note about it.

if ((nrow(rangeoutliers) + nrow(statoutliers) + nrow(dups_fail) + nrow(blanks_fail))>0){
  
  options(width=10000)
  sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
  cat("WIT Quality Control Log\n\n")
  cat(paste0("Data imported at: ",Sys.time(),"\n"))
  cat(paste0("File: ",file,"\n"))
  cat(paste0("Database table: ",ImportTable,"\n\n"))
  sink()
  if(nrow(blanks_fail)>0){
    blanks_fail <- blanks_fail %>% arrange(ID)
    sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append = T)
    cat(paste0(nrow(blanks_fail)," blanks(s) failed.\n\n"),append=T)
    capture.output(print(blanks_fail[c("ID","Date","Location","BlankCode","Parameter","Units","FinalResult")],print.gap=3,right=F,row.names=F),file=paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
    cat("\n\n")
    sink()
    
  }
  if(nrow(dups_fail)>0){
    dups_fail <- dups_fail %>% arrange(ID)
    sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append = T)
    cat(paste0(nrow(dups_fail)," duplicate(s) outside acceptable RPD.\n\n"),append=T)
    capture.output(print(dups_fail[c("ID","Date","Location","DupCode","Parameter","Units","SampleResult","DuplicateResult")],print.gap=3,right=F,row.names=F),file=paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
    cat("\n\n")
    sink()

  }
  if(nrow(rangeoutliers)>0){
    rangeoutliers <- rangeoutliers %>% arrange(ID)
    sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append = T)
    cat(paste0(nrow(rangeoutliers)," record(s) outside historical range.\n\n"),append=T)
    capture.output(print(rangeoutliers[c("ID","Location","DateTimeET","Parameter","Units","FinalResult","HistoricalMin","HistoricalMean","HistoricalMax")],print.gap=3,right=F,row.names=F),file=paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
    cat("\n\n")
    sink()

  }
  if(nrow(statoutliers)>0){
    statoutliers <- statoutliers %>% arrange(ID)
    sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append = T)
    cat(paste0(nrow(statoutliers)," potential statistical outlier(s) in imported data.\n\n"),append=T)
    capture.output(print(statoutliers[c("ID","Location","DateTimeET","Parameter","Units","FinalResult","HistoricalMin","Percentile25","HistoricalMedian","Percentile75","HistoricalMax")],print.gap=3,right=F,row.names=F),file=paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
    cat("\n\n")
    sink()
    }

  qc_message <- paste0("File ",file," processed at ",Sys.time()," with QC warning. See QC Log ",paste0(ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
  
      } else {
        
      # If no historical or statistical outliers, prints line to WIT log
          qc_message <- NA
        
      }

### Returns output message from function for email.
return(qc_message)
  
}

##########################################################################################

# qc_message <- QCCHECK(df.qccheck=df.wq,file=file,ImportTable=ImportTable)
