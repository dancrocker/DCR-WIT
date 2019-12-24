
################################### HEADER ###################################
#  TITLE: WIT QC Test
#  DESCRIPTION: Compare imported data to historical ranges and percentiles. If outliers found, produces QC log and outputs text for email.
#  AUTHOR(S): Travis Drury
#  DATE LAST UPDATED: 12/17/19
#  GIT REPO:
#  R version 3.6.0 (2019-04-26)  i386
##############################################################################

###  Load required dplyr package
library(dplyr)

### Function compares df.qccheck records (in the format at the end of WIT processing before importing) to historical min/max and percentiles

QCCHECK <- function(df.qccheck,file,ImportTable){

### FETCH CACHED DATA FROM WAVE RDS FILES ####
  datadir <- config[1]
 ### Make a list of all the .rds files using full path
 rds_files <- list.files(datadir,full.names = TRUE ,pattern = '\\.rds$')
 ### Select which rds files needed for this script
 rds_in <- c(38,34) # EDIT RDS FILES NEEDED HERE
 ### subset rds files (if you want all of them then skip rds_in and the following line)
 rds_files <- rds_files[rds_in]
 ### create an object that contains all of the rds files
 data <- lapply(rds_files, readRDS)
 ### Make a list of the df names by eliminating extension from files
 df_names <- gsub('.rds', '', list.files(datadir, pattern = '\\.rds$'))
 df_names <- df_names[rds_in]
 # name each df in the data object appropriately
 names(data) <- df_names
 ### Extract each element of the data object into the global environment
 list2env(data ,.GlobalEnv)
 ### Remove data
 rm(data)  
 ### Round 
  
### Create empty dataframes for QC results with output column names
statoutliers <- df.qccheck[NULL,names(df.qccheck)]
statoutliers<-mutate(statoutliers, Percentile25=NA, HistoricalMedian=NA, Percentile75=NA, IQR=NA)

rangeoutliers <- df.qccheck[NULL,names(df.qccheck)]
rangeoutliers<-mutate(rangeoutliers, HistoricalMin=NA, HistoricalMean=NA, HistoricalMax=NA)


### Loop to compare results with historical min/max and percentiles

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
  
    # Only runs QC check on each result if the location/parameter/unit combination in the imported data is present in the summary RDS file, else will skip that record and proceed to next record
  if (df.qccheck$Location[i] %in% dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$Site
        & df.qccheck$Parameter[i] %in% dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$Parameter
        & df.qccheck$Units[i] %in% dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$Units){
      
    # Add to rangeoutliers if less than historical minimum for that site/parameter    
  if (df.qccheck$FinalResult[i] < dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$Min){
    rangeoutliers <- df.qccheck[i,] %>%
      cbind (trib_wach_summary %>%
               filter (Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i]) %>%
               select (one_of(c("Min","Mean","Max"))) %>%
               rename (HistoricalMin = Min, 
                       HistoricalMean = Mean, 
                       HistoricalMax = Max)) %>%
              bind_rows (rangeoutliers)
  } else{

    # Add to rangeoutliers if greater than historical maximum for that site/parameter            
    if (df.qccheck$FinalResult[i] > dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$Max){
      rangeoutliers <- df.qccheck[i,] %>%
        cbind (trib_wach_summary %>%
                 filter (Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i]) %>%
                 select (one_of(c("Min","Mean","Max"))) %>%
                 rename (HistoricalMin = Min, 
                         HistoricalMean = Mean, 
                         HistoricalMax = Max)) %>%
                bind_rows (rangeoutliers)
    } else{

      # Add to statoutliers if FinalValue is less than (25th percentile - 1.5 times the interquartile range) that site/parameter 
      if (df.qccheck$FinalResult[i] < ((dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$percentile25)-(1.5*(dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$IQR)))){
        statoutliers <- df.qccheck[i,] %>%
          cbind (trib_wach_summary %>%
                   filter (Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i]) %>%
                   select (one_of(c("percentile25","Median","percentile75","IQR"))) %>%
                   rename (Percentile25 = percentile25, 
                           HistoricalMedian = Median, 
                           Percentile75 = percentile75,
                           IQR = IQR)) %>%
                  bind_rows (statoutliers)
      } else{

        # Add to statoutliers if FinalValue is greater than (75th percentile + 1.5 times the interquartile range) that site/parameter        
        if (df.qccheck$FinalResult[i] > ((dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$percentile75)+(1.5*(dplyr::filter(trib_wach_summary,Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i])$IQR)))){
          statoutliers <- df.qccheck[i,] %>%
                    cbind (trib_wach_summary %>%
                     filter (Site==df.qccheck$Location[i],Parameter==df.qccheck$Parameter[i],Units==df.qccheck$Units[i]) %>%
                     select (one_of(c("percentile25","Median","percentile75","IQR"))) %>%
                     rename (Percentile25 = percentile25, 
                             HistoricalMedian = Median, 
                             Percentile75 = percentile75,
                             IQR = IQR)) %>%
                    bind_rows (statoutliers)
        }
      }}}}}}}}


### Print results of outlier check to unique WIT log

  # Delete a log of the same name if it exists
  if (file.exists(paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))){
    file.remove(paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
  }

  # If values exist outside historical range for site/parameter/units, print QC log file, create message for email.
  if (nrow(rangeoutliers)>0 & nrow(statoutliers)==0){
    sink(file = paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
    cat("WIT Quality Control Log\n\n")
    cat(paste0("Data imported at: ",Sys.time(),"\n"))
    cat(paste0("File: ",file,"\n"))
    cat(paste0("Database table: ",ImportTable,"\n\n"))
    cat(paste0(nrow(rangeoutliers)," record(s) outside historical range.\n\n"),append=T)
    capture.output(print(rangeoutliers[c("ID","Location","SampleDateTime","Parameter","Units","FinalResult","HistoricalMin","HistoricalMean","HistoricalMax")],print.gap=3,right=F,row.names=F),file=paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
    cat("\n\n")
    sink()
    qc_message <- paste0("File ",file," processed at ",Sys.time(),". ",nrow(rangeoutliers)," record(s) outside historical range. See QC Log ",paste0(ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
    
  } else{
    
    # If values exist outside statistical range for site/parameter/units, print QC log file
    if (nrow(rangeoutliers)==0 & nrow(statoutliers)>0){
      sink(file = paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
      cat("WIT Quality Control Log\n\n")
      cat(paste0("Data imported at: ",Sys.time(),"\n"))
      cat(paste0("File: ",file,"\n"))
      cat(paste0("Database table: ",ImportTable,"\n\n"))
      cat(paste0(nrow(statoutliers)," potential statistical outlier(s) in imported data.\n\n"),append=T)
      capture.output(print(statoutliers[c("ID","Location","SampleDateTime","Parameter","Units","FinalResult","Percentile25","HistoricalMedian","Percentile75")],print.gap=3,right=F,row.names=F),file=paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
      cat("\n\n")
      sink()
      qc_message <- paste0("File ",file," processed at ",Sys.time(),". ",nrow(statoutliers)," potential statistical outlier(s) identified. See QC Log ",paste0(ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
      
    } else{
      
      # If values exist outside both historical and statistical ranges for site/parameter/units, print QC log file
      if (nrow(rangeoutliers)>0 & nrow(statoutliers)>0){
        sink(file = paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
        cat("WIT Quality Control Log\n\n")
        cat(paste0("Data imported at: ",Sys.time(),"\n"))
        cat(paste0("File: ",file,"\n"))
        cat(paste0("Database table: ",ImportTable,"\n\n"))
        cat(paste0(nrow(rangeoutliers)," record(s) outside historical range.\n\n"),append=T)
        capture.output(print(rangeoutliers[c("ID","Location","SampleDateTime","Parameter","Units","FinalResult","HistoricalMin","HistoricalMean","HistoricalMax")],print.gap=3,right=F,row.names=F),file=paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
        sink()
        sink(file = paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append = T)
        cat(paste0("\n\n",nrow(statoutliers)," potential statistical outlier(s) in imported data.\n\n"),append=T)
        capture.output(print(statoutliers[c("ID","Location","SampleDateTime","Parameter","Units","FinalResult","Percentile25","HistoricalMedian","Percentile75")],print.gap=3,right=F,row.names=F),file=paste0(config[28],"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
        cat("\n\n")
        sink()
        qc_message <- paste0("File ",file," processed at ",Sys.time(),". ",nrow(rangeoutliers)," record(s) outside historical range and ",nrow(statoutliers)," potential statistical outlier(s) identified. See QC Log ",paste0(ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
        
      } else {
        
      # If no historical or statistical outliers, prints line to WIT log
          qc_message <- NA
        
      }
    }}

### Returns output message from function for email.
return(qc_message)
  
}

##########################################################################################

# qc_message <- QCCHECK(df.qccheck=df.wq,file=file,ImportTable=ImportTable)
