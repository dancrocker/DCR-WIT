################################### HEADER ###################################
#  TITLE: outlook_email.R
#  DESCRIPTION: A function that will send an email through MS outlook
#  AUTHOR(S): Dan Crocker 
#  DATE LAST UPDATED: November 20, 2019
#  GIT REPO: DCR-WIT
#  R version 3.5.3 (2019-03-11)  i386
##############################################################################.


########################################################################.
###                  EMAIL THROUGH OUTLOOK                          ####
########################################################################.

# install.packages("RDCOMClient", repos = "http://www.omegahat.net/R") # Install Repo

library(RDCOMClient)

OL_EMAIL <- function(to, cc = "", bcc = "", subject, body){

# Open Outlook
Outlook <- COMCreate("Outlook.Application")

# Create a new message
Email = Outlook$CreateItem(0)

# Set the recipient, subject, and body
Email[["to"]] =  to # semi-colon separated email addresses as string no <>
Email[["cc"]] = cc
Email[["bcc"]] = bcc
Email[["subject"]] = subject
Email[["body"]] = body

# Send the message
Email$Send()

# Close Outlook, clear the message
rm(Outlook, Email)


return(glue("Email notification was sent to {to}."))
}





