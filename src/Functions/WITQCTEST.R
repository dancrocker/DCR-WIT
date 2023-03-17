
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

### Function compares df.qccheck records (in the format at the end of WIT processing before importing) to historical min/max and percentiles

QCCHECK <- function(df.qccheck, file, ImportTable){

### FETCH CACHED DATA FROM WAVE RDS FILES ####
  
  files <-  c("df_wach_param.rds", "trib_wach_summary.rds")
  
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


### Print results of outlier check to unique WIT log
QC_log_dir <- paste0(wach_team_root, config[["QC_Logfiles"]])
  # Delete a log of the same name if it exists
  if (file.exists(paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))){
    file.remove(paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
  }

  # If values exist outside historical range for site/parameter/units, print QC log file, create message for email.
  if (nrow(rangeoutliers)>0 & nrow(statoutliers)==0){
    options(width=10000)
    rangeoutliers <- rangeoutliers %>% arrange(ID)
    sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
    cat("WIT Quality Control Log\n\n")
    cat(paste0("Data imported at: ",Sys.time(),"\n"))
    cat(paste0("File: ",file,"\n"))
    cat(paste0("Database table: ",ImportTable,"\n\n"))
    cat(paste0(nrow(rangeoutliers)," record(s) outside historical range.\n\n"),append=T)
    capture.output(print(rangeoutliers[c("ID","Location","DateTimeET","Parameter","Units","FinalResult","HistoricalMin","HistoricalMean","HistoricalMax")],print.gap=3,right=F,row.names=F),file=paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
    cat("\n\n")
    sink()
    qc_message <- paste0("File ",file," processed at ",Sys.time(),". ",nrow(rangeoutliers)," record(s) outside historical range. See QC Log ",paste0(ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
    
  } else{
    
    # If values exist outside statistical range for site/parameter/units, print QC log file
    if (nrow(rangeoutliers)==0 & nrow(statoutliers)>0){
      options(width=10000)
      statoutliers <- statoutliers %>% arrange(ID)
      sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
      cat("WIT Quality Control Log\n\n")
      cat(paste0("Data imported at: ",Sys.time(),"\n"))
      cat(paste0("File: ",file,"\n"))
      cat(paste0("Database table: ",ImportTable,"\n\n"))
      cat(paste0(nrow(statoutliers)," potential statistical outlier(s) in imported data.\n\n"),append=T)
      capture.output(print(statoutliers[c("ID","Location","DateTimeET","Parameter","Units","FinalResult","HistoricalMin","Percentile25","HistoricalMedian","Percentile75","HistoricalMax")],print.gap=3,right=F,row.names=F),file=paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
      cat("\n\n")
      sink()
      qc_message <- paste0("File ",file," processed at ",Sys.time(),". ",nrow(statoutliers)," potential statistical outlier(s) identified. See QC Log ",paste0(ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"))
      
    } else{
      
      # If values exist outside both historical and statistical ranges for site/parameter/units, print QC log file
      if (nrow(rangeoutliers)>0 & nrow(statoutliers)>0){
        options(width=10000)
        rangeoutliers <- rangeoutliers %>% arrange(ID)
        statoutliers <- statoutliers %>% arrange(ID)
        sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
        cat("WIT Quality Control Log\n\n")
        cat(paste0("Data imported at: ",Sys.time(),"\n"))
        cat(paste0("File: ",file,"\n"))
        cat(paste0("Database table: ",ImportTable,"\n\n"))
        cat(paste0(nrow(rangeoutliers)," record(s) outside historical range.\n\n"),append=T)
        capture.output(print(rangeoutliers[c("ID","Location","DateTimeET","Parameter","Units","FinalResult","HistoricalMin","HistoricalMean","HistoricalMax")],print.gap=3,right=F,row.names=F),file=paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
        sink()
        sink(file = paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append = T)
        cat(paste0("\n\n",nrow(statoutliers)," potential statistical outlier(s) in imported data.\n\n"),append=T)
        capture.output(print(statoutliers[c("ID","Location","DateTimeET","Parameter","Units","FinalResult","HistoricalMin","Percentile25","HistoricalMedian","Percentile75","HistoricalMax")],print.gap=3,right=F,row.names=F),file=paste0(QC_log_dir,"/",ImportTable,"_",file,"_",format(Sys.Date(),"%Y-%m-%d"),".txt"),append=T)
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
