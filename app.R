##############################################################################################################################
#     Title: app.R
#     Type: Master file DCR Import Data Shiny App
#     Description: This Shiny App contains the "master" script for the Import Data app. The app contains a ui and server component
#           and sources R scripts from the App folder
#     Written by: Nick Zinck, Dan Crocker
      # Last Update: May, 2018
##############################################################################################################################
# Notes:
#   1. If running locally the config file must be loaded first (see WAVE_WIT_Local script)

#options(shiny.reactlog = TRUE) # This is a visual representation of reactivity

####################################################################################################
# Load Libraries and Script (Sources, Modules, and Functions)
####################################################################################################
print(paste0("WIT App lauched at ", Sys.time()))
### Load packages

ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE, repos="http://cran.rstudio.com/")
  sapply(pkg, require, character.only = TRUE)
}
packages <- c("shiny", "shinyjs", "shinythemes", "readxl", "dplyr", "tidyr", "tidyverse", "RODBC", "odbc", "DBI", "lubridate",
              "DescTools", "devtools", "scales", "data.table", "magrittr", "stringr", "openxlsx", "V8", "installr",
              "sendmailR", "data.table", "dataRetrieval","httpuv", "rlang")

# Envoke every so often to update packages
# update.packages(lib.loc = config[15] , repos ="http://cran.rstudio.com/", oldPkgs = c(packages, "dplyr"), ask = F)

# Load-Install Packages
ipak(packages)

# Connect to db for queries below
con2 <- dbConnect(odbc::odbc(),
                  .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}",
                                             paste0("DBQ=", config[3]), "Uid=Admin;Pwd=;", sep = ";"),
                  timezone = "America/New_York")
# Set user info
user <-  Sys.getenv("USERNAME")
userdata <- readxl::read_xlsx(path = config[17])
username <- paste(userdata$FirstName[userdata$Username %in% user],userdata$LastName[userdata$Username %in% user],sep = " ")
useremail <- userdata$Email[userdata$Username %in% user]
userlocation <- userdata$Location[userdata$Username %in% user]
# Specify mail server
MS <- config[5]

### Set Location Dependent Variables - datatsets and distro
if (userlocation == "Wachusett") {
  datasets <-  read_excel(config[8], sheet = 1, col_names = T, trim_ws = T) %>%
  filter(ImportMethod == "Importer-R")
} else {
  if (userlocation == "Quabbin") {
    datasets <-  read_excel(config[9], sheet = 1, col_names = T, trim_ws = T) %>%
    filter(ImportMethod == "Importer-R")
  } else {
    datasets <-  read_excel(config[10], sheet = 1, col_names = T, trim_ws = T) %>% # This is a placeholder for BK forestry data... to be deprecated
      filter(ImportMethod == "Importer-R")
  }
}

flagdatasets <- filter(datasets, !is.na(FlagTable))

flags <- dbReadTable(con2, "tblFlags") %>%
  select(-3)
flags$label <- paste0(flags$Flag_ID," - ", flags$FlagDescription)

# Disconnect and remove connection
dbDisconnect(con2)
rm(con2)
actionCount <- reactiveVal(0)

###################################################################################
##################################  User Interface  ###############################
###################################################################################

ui <- tagList(
      useShinyjs(),
      div(
      id = "form", # open div
      navbarPage("WQ DATA IMPORTER",
        tabPanel("Import Data",
          fluidPage(theme = shinytheme("slate"),
                fluidRow(
                  column(1,
                    actionButton("refresh", "REFRESH"),
                    br()
                  ),
                  column(11,
                    h2("Import Data To Databases", align = "center"),
                    br()
                  )
#############################################################
                ),  # End Fluid Row
                fluidRow(
                  column(6,
                         wellPanel(
                           selectInput("datatype", h4("1. Select data type:"),
                                       choices = datasets$DataType),
                           br()
                         )
                  ),
                  column(6,
                         wellPanel(
                           uiOutput("file.UI")
                         ),
                         uiOutput("probe.UI")
                  )
                ), # End Fluid Row
                fluidRow(
                  column(12,
                         wellPanel(
                           strong(h4("3. Run the 'Process Data' script:")),
                           br(),
                           uiOutput("process.UI"),
                           br(),
                           h4(textOutput("text.process.status"))
                         ),
                         wellPanel(
                           strong(h4("4. Run the 'Import Data' script to upload processed data to DB:")),
                           br(),
                           uiOutput("import.UI"),
                           br(),
                           uiOutput("text.import.status")
                         ),
                         tabsetPanel(
                           tabPanel("Processed WQ Data",
                                    dataTableOutput("table.process.wq")
                           ),
                           tabPanel("Processed Flag Index Data",
                                    dataTableOutput("table.process.flag")
                           ) # End Tab Panel
                         ) # End Tabset Panel
                  ) # End Col
                ) # End Fluid row
          )  #End Fluid Page
        ), # End Tab panel
        tabPanel("Manually Flag Data",
                 fluidPage(theme = shinytheme("slate"),
                           fluidRow(
                                column(1,
                                  actionButton("refresh2", "REFRESH"),
                                  br()
                                ),
                                column(11,
                                  h2("Flag Records in Databases", align = "center"),
                                  br()
                                )
                           ),
                           fluidRow(
                             column(6,
                                    wellPanel(
                                      selectInput("flagdatatype", h4("1. Select data type:"),
                                                  choices = flagdatasets$DataType),
                                                  selected = 1,
                                      br()
                                    )
                             ),
                             column(6,
                                    wellPanel(
                                      selectInput("flag", h4("2. Select flag to apply to records:"),
                                                  choices = flags$label),
                                      textOutput("flagSelected"),
                                      br(),
                                      strong("Note: If a new flag is needed please contact database administrator to add the flag to the database")
                                    )
                             )
                          ),
                          fluidRow(h4("3. Choose Samples to Flag:"),
                             column(4,
                                    wellPanel(
                                      fluidRow(
                                        column(12,
                                               strong("A. Enter record numbers individually:" )
                                        )
                                      ),
                                      br(),
                                      textInput("flagsA","--- (Separate with commas)"),
                                      textOutput("A"),
                                      br()
                                    )
                            ),
                            column(4,
                                   wellPanel(
                                      fluidRow(
                                        column(12,
                                          strong("B. Enter a continuous range:"),
                                          strong("--- (No gaps, values inclusive)"),
                                            br()
                                        )
                                     ), # End Well panel
                                     fluidRow(
                                       br(),
                                       column(5,
                                              numericInput("flagsB1", "Record Range Minimum:", NULL)
                                       ),
                                       column(2,
                                              h4("To", align = "center")
                                       ),
                                       column(5,
                                              numericInput("flagsB2", "Record Range Maximum:", NULL)
                                       )
                                     ),
                                     textOutput("B"),
                                     br()
                                   ) # End Well panel
                            ),
                            column(4,
                                  wellPanel(
                                      strong("C. Use a list of record numbers from a file:"),
                                      tags$hr(),
                                      checkboxInput("header", "Check if column has a header", FALSE),
                                      fileInput("flagsC", "--- Click 'Browse' to a CSV file", accept = ".csv",
                                                buttonLabel = "Browse...", placeholder = "No file selected"),
                                      strong("Note: The file must be a .csv with number values in the first column."),
                                      textOutput("C"),
                                      br()
                                  ) # End Well Panel
                            )# End Column
                        ), # End Fluid Row
                        fluidRow(column(6,
                                    wellPanel(
                                      strong(h4("4. Prepare flag data for import:")),
                                      br(),
                                      uiOutput("processflags.UI"),
                                      br(),
                                      h4(textOutput("D")),
                                      textOutput("flagRec"),
                                      br()
                                    )
                                ),
                                column(6,
                                    wellPanel(
                                      strong(h4("6. Import flag data :")),
                                      br(),
                                      uiOutput("importFlags.UI"),
                                      br(),
                                      h4(textOutput("text.Flagimport.status"))
                                    )
                                )
                        ),
                        fluidRow(column(12,
                                        strong(h4("5. Preview flag data before import:")),
                                          # tableOutput("previewtable"),
                                          dataTableOutput("table.manual.flag"),
                                          br()
                                ) # End Column
                        ) # End Fluid Row
                 ) # End Fluid Page
        ) # End Tab Panel
      ) # End NavPage
  ) # Close Div
) # End tagList

########################################################################################
################################    SERVER   ###########################################
########################################################################################

server <- function(input, output, session) {

  ##################################################################################
  #############               IMPORT DATA TAB             ##########################
  ##################################################################################

### Generate function agruments from UI selections

  ds <- reactive({
    filter(datasets, DataType == input$datatype)
  })
  scriptname <- reactive({ # Scripts common to both QB and Wach must be in both src folders!
    req(ds())
    paste0(getwd(), "/src/", userlocation, "/",ds()$ScriptProcessImport[1])
  })
  rawdatafolder <- reactive({
    req(ds())
    ds()$RawFilePath[1]
  })
  ImportTable <- reactive({
    req(ds())
    ds()$TableName[1]
  })
  ImportFlagTable <- reactive({
    req(ds())
    ds()$FlagTable[1]
  })
  processedfolder <- reactive({
    req(ds())
    ds()$ProcessedFilePath[1]
  })

  filename.db <- reactive({
    req(ds())
    ds()$DBPath[1]
  })
  distro1 <- reactive({
    req(ds())
    as.character(ds()$EmailList[1]) %>%
      strsplit(", ")
  })

  ### FILE SELECTION

  # Make the File List
  files <- eventReactive(rawdatafolder() ,{
    grep(x = list.files(rawdatafolder(), ignore.case = T, include.dirs = F),
         # pattern = "^(?=.*\\b(.xlsx|.xlsm)\\b)(?!.*\\$\\b)", # regex to show xlsx files, but filter out lockfiles string = "$"
         pattern = as.character(ds()$RawFileRegEx[1]),
         value = T,
         perl =T)
  })

  # Select Input where user finds and sets the file to import (# this could be set up to do multiple files at a time, but its safer to just do one at a time)
  output$file.UI <- renderUI({
    req(files())
    selectInput(inputId = "file",
                label = "2. Choose file to upload:",
                choices = files())
  })

  # Update Select Input when a file is imported (actually when the import button is pressed (successful or not))
  observeEvent(input$import, {
    updateSelectInput(session = session,
                      inputId = "file",
                      label = "2. Choose file to upload:",
                      choices = files(),
                      selected = input$file)
  })

  # Show Probe Option when Profile Data Selected
  output$probe.UI <- renderUI({
    req(input$datatype == "Profiles")
    selectInput(inputId = "probe",
                label = "Choose Probe type used for this data:",
                choices = c("Hydrolab Datasonde3 H20",
                            "YSI_EXO2",
                            "DEP YSI",
                            "Hydrolab MS5",
                            "Hydrolab Surveyor II"),
                selected = "YSI_EXO2")
  })

  ### Process DATA

  # Process Action Button
  output$process.UI <- renderUI({
    req(input$file)
    actionButton(inputId = "process",
                 label = paste0('Process "', input$file, '" Data'),
                 width = '500px')
  })

  # Run the function to process the data and return 2 dataframes and path as list
  dfs <- eventReactive(input$process,{
    source(scriptname(), local = T) # Hopefully this will overwrite functions as source changes...needs more testing
    PROCESS_DATA(file = input$file, rawdatafolder = rawdatafolder(), filename.db = filename.db(),
                 probe = input$probe, ImportTable = ImportTable(), ImportFlagTable = ImportFlagTable())
  })

  # Extract each dataframe
  df.wq <- reactive({
            dfs()[[1]]
        })
  path  <- reactive({
            dfs()[[2]]
        })
  df.flags  <- reactive({
            dfs()[[3]]
        })

  # Last File to be Processed
  file.processed <- eventReactive(input$process, {
    input$file
  })

  # Text for Process Data Error or Successful
  process.status <- reactive({
    if(input$file != file.processed()){
      " "
    }else if(inherits(try(df.wq()), "try-error")){
      geterrmessage()
    }else{
      paste0('The file "', input$file, '" was successfully processed')
    }
  })

  # Text Output
  output$text.process.status <- renderText({process.status()})

  # Show import button and tables when process button is pressed
  # Use of req() later will limit these to only show when process did not create an error)
  observeEvent(input$process, {
    show('import')
    # show('table.process.wq')
    # show('table.process.flag')
  })

  ### Import Data

  # Import Action Button - Will only be shown when a file is processed successfully
  output$import.UI <- renderUI({
    req(try(df.wq()))
    actionButton(inputId = "import",
                 label = paste("Import", file.processed(), "Data"),
                 width = '500px')
  })

  # Import Data - Run import_data function
  observeEvent(input$import, {
    source(scriptname(), local = T)
    out <- tryCatch(IMPORT_DATA(df.wq = df.wq(),
                                df.flags = df.flags(),
                                path = path(),
                                file = input$file,
                                filename.db = filename.db(),
                                processedfolder = processedfolder(),
                                ImportTable = ImportTable(),
                                ImportFlagTable = ImportFlagTable())
                    ,
                    error=function(cond) {
                      message(paste("There was an error, data not imported...\n", cond))
                      return(NA)
                    },
                    warning=function(cond) {
                      message(paste("Import process completed with warnings...\n", cond))
                      return(NULL)
                    },
                    finally={
                      message(paste("Import Process Complete"))
                    }
                  )
          ImportFailed <- any(class(out) == "error")

          if (ImportFailed == TRUE){
            print(paste0("Import Failed at ", Sys.time() ,". There was an error: "))
            print(out)
          } else {
            print(paste0("Data Import Successful at ", Sys.time()))
            NewCount <- actionCount() + 1
            actionCount(NewCount)
            print(paste0("Action Count was ", actionCount()))
            ImportEmail()
          }
  })

  ### Function to send ImportEmail
  ImportEmail <- function() {
    out <- tryCatch(
      message("Trying to send email"),
      sendmail(from = paste0("<",useremail,">"),
               to = distro1(),
               subject = paste0("New Data has been Imported to a ", userlocation," Database"),
               msg = paste0(username," has imported ", nrow(df.wq()), " new record(s) for the dataset: ",
                            input$datatype, " | Filename = ", input$file),
               control=list(smtpServer=MS))
      ,
      error=function(cond) {
        message(paste("User cannot connect to SMTP Server, cannot send email", cond))
        return(NA)
      },
      warning=function(cond) {
        message(paste("Send mail function caused a warning, but was completed successfully", cond))
        return(NULL)
      },
      finally={
        message(paste("Import email was sent successfully"))
      }
    )
    return(out)
  }

  # Hide import button and tables when import button is pressed (So one cannot double import same file)
  observeEvent(input$import, {
    hide('import')
    #hide('table.process.wq')
    #hide('table.process.flag')
  })

  # Create a delayed reactive to trigger input file change update after import
  ### THIS GETS TRIGGERED TOO QUICKLY - NEEDS TO TRIGGER AFTER INPUT$IMPORT IS CLICKED, NOT ON A TIMER
  # import.delay <- reactive({
  #   # Delay reactive invalidation (in milliseconds)
  #   invalidateLater(10000, session)
  #   input$import
  # })

  # Add text everytime successful import
  observeEvent(input$import, {
    insertUI(
      selector = "#import",
      where = "afterEnd",
      ui = h4(paste("Successful import of", nrow(df.wq()), "record(s) in", input$file, "to", input$datatype, "Database"))
    )
  })

  ### Table Outputs

  # Processed WQ Table - Only make table if processing is successful
  output$table.process.wq <- renderDataTable({
    req(try(df.wq()))
    df.wq()
  })

  # Processed Flag Table - Only make table if processing is successful
  output$table.process.flag <- renderDataTable({
    req(try(df.flags()))
    df.flags()
  })


  ##################################################################################
  #############               MANUAL FLAG TAB             ##########################
  ##################################################################################

  ### Generate function agruments from UI selections

  dsflags <- reactive({
    filter(flagdatasets, DataType == input$flagdatatype)
  })
  flag.db <- reactive({
    # req(dsflags())
    dsflags()$DBPath[1]
  })
  datatable2 <- reactive({
    # req(dsflags())
    dsflags()$TableName[1]
  })
  flagtable <- reactive({
    # req(dsflags())
    dsflags()$FlagTable[1]
  })
  flagSelected <- reactive({
    req(input$flag)
    as.numeric(substr(input$flag, 1, 3))
    })

  distro2 <- reactive({
    # req(dsflags())
    as.character(dsflags()$EmailList[1]) %>%
      strsplit(", ")
  })

  flagsA <- reactive({
    # req(isTruthy(input$flagsA))
    if(isTruthy(input$flagsA)){
      x <- str_split(input$flagsA,",") %>%
        lapply(function(x) as.numeric(x))
      as.vector(as.integer(unlist(x)))
    }
    else{
      NA
    }
  })

  flagsB <- reactive({
    # req(isTruthy(input$flagsB1)) ### This line prevented the preview data UI from displaying
    if(isTruthy(input$flagsB1) & isTruthy(input$flagsB2)){
      as.vector(as.integer(seq.int(input$flagsB1, input$flagsB2, 1)))
    }
    else{
      NA
    }
  })

  flagsC <- reactive({
    # req(input$flagsC)
    if(isTruthy(input$flagsC)){
      inFile <- input$flagsC
      as.integer(unlist(as.vector(read.csv(inFile$datapath, header = input$header))))
    } else{
      NA
    }
  })

  flagRecords <- reactive({
    x <- list(flagsA(),flagsB(),flagsC())
    combined <- lapply(x, function(x) x[!is.na(x)])
    flagRecords <- unique(Reduce(c,combined))
  })

  # Preview each output
  output$A <- renderText({
    req(input$flagsA)
    paste0(length(flagsA()), " records have been marked for flagging: ", list(flagsA()))
  })

  output$B <- renderText({
    req(input$flagsB1, input$flagsB2)
    paste0(length(flagsB()), " records have been marked for flagging: A continuous range - ", list(flagsB()))
  })

  output$C <- renderText({
    req(input$flagsC)
    paste0(length(flagsC()), " records have been marked for flagging: ", list(flagsC()))
  })

  output$D <-
    renderText({
      req(isTruthy(input$flagsA) | isTruthy(input$flagsB1) & isTruthy(input$flagsB2) | isTruthy(input$flagsC))
      paste0(length(flagRecords()), " records have been marked for flagging: ", list(flagRecords()))
    })

  ### Process DATA

  # Process Manual Flags Button
  output$processflags.UI <- renderUI({
    req(isTruthy(input$flagsA) | isTruthy(input$flagsB1) & isTruthy(input$flagsB2) | isTruthy(input$flagsC))
    actionButton(inputId = "processflags",
                 label = "Prepare flags for import",
                 width = '500px')
  })

   # # Run the function to process
  df.manualflags <- eventReactive(input$processflags, {
    source(paste0(getwd(), "/src/", userlocation, "/ImportManualFlags.R"), local = T) # Hopefully this will overwrite functions as source changes...needs more testing
    PROCESS_DATA(flag.db = flag.db() , datatable = datatable2(), flagtable = flagtable(), flag = flagSelected(), flagRecords = flagRecords())
  })

  # Processed Flag Table - Only make table if processing is successful
  output$table.manual.flag <- renderDataTable({
    req(try(df.manualflags()))
    df.manualflags()
  })

  # Import Action Button - Will only be shown when a file is processed successfully
  output$importFlags.UI <- renderUI({
    req(try(df.manualflags()))
    actionButton(inputId = "importFlags",
                 label = paste0("Import ", length(flagRecords()), " database record flags for Flag ID:   ", input$flag),
                 width = '500px')
  })

  # Import Data - Run import_data function
  observeEvent(input$importFlags, {
    source(paste0(getwd(), "/src/", userlocation, "/ImportManualFlags.R"), local = T)
    out <- tryCatch(IMPORT_DATA(flag.db = flag.db(),
                        flagtable = flagtable(),
                        df.manualflags = df.manualflags()),
                        error = function(e) e)

    ImportFailed <- any(class(out) == "error")

    if (ImportFailed == TRUE){
      print(paste0("Flag Import Failed at ", Sys.time(), ". There was an error: "))
      print(out)
    } else {
      print(paste0("Flag Import Successful at ", Sys.time()))
      NewCount <- actionCount() + 1
      actionCount(NewCount)
      print(actionCount())
      FlagEmail()
    }
  })

# Function to send FlagEmail
  FlagEmail <- function() {
      out <- tryCatch({
          message("Trying to send email")
          sendmail(from = paste0("<",useremail,">"),
                   to = distro2(),
                   subject = paste0("Data has been flagged in a ", userlocation," Database"),
                   msg = paste0(username," has flagged ", length(flagRecords()), " existing record(s) for the dataset: ",
                                input$flagdatatype, ", with flag ", input$flag),
                   control=list(smtpServer=MS))
        },
        error=function(cond) {
          message(paste("User cannot connect to SMTP Server, cannot send email"))
          return(NA)
        },
        warning=function(cond) {
          message(paste("Send mail function caused a warning, but was completed successfully"))
          return(NULL)
        },
        finally={
          message("Import Process Complete")
        }
      )
      return(out)
    }

  # Add text everytime successful import
  observeEvent(input$importFlags, {
    insertUI(
      selector = "#importFlags",
      where = "afterEnd",
      ui = h4(paste("Successful import of", nrow(df.manualflags()), " flag record(s) to", flagtable(), "in", flag.db(), "Database"))
    )
  })

  observeEvent(input$importFlags, {
    hide('importFlags')
    #hide('table.process.wq')
    #hide('table.process.flag')
  })

  # Text for Process Flag Data Error or Successful
  # processFlag.status <- reactive({
  #   if(input$file != file.processed()){
  #     " "
  #   }else if(inherits(try(df.wq()), "try-error")){
  #     geterrmessage()
  #   }else{
  #     paste0('The file "', input$file, '" was successfully processed')
  #   }
  # })

  observeEvent(input$refresh, {
    shinyjs::reset("form")
  })

  observeEvent(input$refresh2, {
    shinyjs::reset("form")
  })

  ######################################################
  # Stop app when browser session window closes
  session$onSessionEnded(function() {
          # If data was successfully imported/flagged, then actionCount should be > 0; Update data files for WAVE App

    if (isolate(actionCount()) > 0) {
              print("Action Count was > 0, new data available in databases; Running the updateWAVE script to cache new .rds files")
              shell.exec(config[11])
          } else {
            print("Action Count was 0, data not modified in databases; New .rds files will not be generated")
          }
    print(paste0("WIT session ended at ", Sys.time()))
    stopApp()
  })

} # end server function

#combines the user interface and server (it's a must)
shinyApp(ui = ui, server = server)

