#+++++++++++++++++++++++++JOB_EXPORT_OPEN_DATASETS+++++++++++++++++++++++++++++++
#This is a script to automate the proces of updating published open dataset on 
  #the open data portal of the Flemish Government.

#Connection data is stored in the config file.

#This script relies on both R and python to establish this.

#There are a number of steps:
  #0 - Initialization ==>setting working directories and loading packages
  #1 - Data Extraction ==> connection to databases running SQL and export to CSV
  #2 - Transfer of the CSV to the FTP-server
  #3 - Update metadata on the open data portal (CKAN)
  #4 - Reporting, archiving and sending E-mail

#General functionality:
    #The script works as follows. A connection is made to the database and SQL-queries are
      #run against. Results are fetched and exported as CSV-files on the local pc.
    #For each dataset also its metadata is modified. Each datasets has its own metadata file
      #which is in JSON-format.
    #This script only modifies the timestamp at which the dataset was updated.
    #After extraction and updating the metadata, the CSV-files are transfered to the ftp-server.
      #Since this was more easy to achieve in python, we used a python script to carry out this task.
      #The python script is called from within R using the package PythonInR.
    #Once the CSV-files are transfered also the metada is updated by sending over the JSON-files to
      #the open data portal. We also use a python script to achieve this. This script is also run
      #from within R.
    #At the end, statistics are gathered as to the numberof files transfered and the number of SQL run.
    #A mail is sent to the open data coordinator notifying the end of the job.

#Data-extraction:

    #For the moment only one database is involved: the energy performance database
      #which is an Oracle database.
    
    #The connection to this database relies on the R-package "ROracle". 
      #Apparently this runs only well when we enable the 64 bit R-installation.
      #We can do this from RStudio by going to Tools > Global Options > General.
        #There we can change the R-version: 64 bit or 32 bit.
      #Note that this script is run in the windows task scheduler. There we can
        #select the program "Rscript.exe" to run this script. So we automatically can
        #choose the 64bit version there. We do not need to change the version in Rstudio.
    
    # In order to install the ROracle package we refer to the following thread:
      #https://stackoverflow.com/questions/40232917/how-to-install-roracle-package
    #Install the following zip file "ROracle 1.3-1" as mentioned in the thread. 
    #(URL = http://www.oracle.com/technetwork/database/database-technologies/r/roracle/downloads/index.html)
    
    #In order to establish the connection with the database we relied on 
      #the following tutorial: 
        #https://www.toadworld.com/platforms/oracle/w/wiki/11057.connecting-to-oracle-database-12c-from-oracle-r-with-roracle
    
    #As mentioned above. The script will be scheduled using windows task scheduler.
      #At the end of this script, an email is sent to the outlook account to inform the
        #open data coordinator that the job has finished and the datasets are exported. 
      #Note that in order to be able to receive the emails in outlook we had to enable the 
        #programmatic access to outlook. This is done via:
          #file > options > Trust center > Programmatic Access > Never warn me about....
      #In order to change the setting one has to launch outlook as an administrator.
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#0 - Initialization####
    
    #Clear workspace
    
    rm(list=ls())
    
    #Load the required packages
    
    library("ROracle")
    library("data.table")
    library("jsonlite")
    
    #Set wd to the scripts directory since we have to import our config file

    setwd("C:/Users/reynsni/OneDrive voor Bedrijven/01_Open_Data/00_Scripts")

    #Import the config file

    config <- fromJSON(
      txt = "config.json"
      )
    

    #Set the working directories
    
    wd_scripts <- config$test_wd_scripts
    wd_sql <- config$test_wd_sql
    wd_csv <- config$test_wd_csv
    wd_json <- config$test_wd_json

#1 - Data Extraction####
    
    #Set working directory to directory where SQL-scripts reside
    
    setwd(wd_sql)
    
    #Load the oracle driver
    
    drv <- dbDriver("Oracle")
    
    #Create a connection string.
      #We copied the script from the following tutorial:
        #https://www.toadworld.com/platforms/oracle/w/wiki/11057.connecting-to-oracle-database-12c-from-oracle-r-with-roracle
      #If the connection detail should change, one needs to modify the host, post and sid and nothing else.
    
    host <- config$db_oracle_host
    port <- as.integer(config$db_oracle_port) #We convert the string to integer
    sid <- config$db_oracle_sid
    
    connect.string <- paste(
      "(DESCRIPTION=",
      "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
      "(CONNECT_DATA=(SID=", sid, ")))", sep = "")
    
    #Creat the connection to the database  
    
    connection <- dbConnect(
                        drv, 
                        username = config$db_oracle_user_name, 
                        password = config$db_oracle_password,
                        dbname = connect.string
                        )
    
    
    #Grab all SQL-files in our working directory
    
     sql_files <- list.files()[grep(x = list.files(), pattern = ".sql")]
    
    
    #Loop over the indexes of the sql-files in sql_files.
     #For each file we execute the SQL on the database and fetch the results.
     
     for(i in 1:length(sql_files)){
     
       #Reset working directory to the directory where the SQL-scripts reside
       
       setwd(wd_sql)
         
       #Capture full dataset name (= ID of the dataset + dataset name)
       
         #We can obtain this full dataset name from the SQL-filenames. We just need 
          #to remove the extension.
         #Locate where the ".sql" extension is located in the filename of the
          #sql query file
       
       pos_extension <- regexpr(pattern = ".sql", text = sql_files[i])[1]
       
       full_dataset_name <- substr(x = sql_files[i], start = 1, stop = pos_extension - 1)
       
        #Load the SQL-statement into variable sql
       
       sql <- paste(readLines(con = sql_files[i]), collapse = " ")
       
       #Run query
       queryresult <- dbSendQuery(connection, sql)
       
       #Fetch Results
       resultdata <- fetch(queryresult)
       
       #Clear resultset
       
       dbClearResult(queryresult)
       
       #Set directory to directory where CSV-files are stored
       
       setwd(wd_csv)
       
       #Write to CSV
       
       csv_file <- paste(full_dataset_name, "csv", sep = ".")
       
       fwrite(
         x = resultdata, 
         file = csv_file, 
         sep = ",", 
         quote = TRUE,
         row.names = FALSE,
         na = "NULL"
         )
       
       
       #Modify the JSON-files containing the metadata.
       #We first need to import the file and then changing the value for the key: "Laatste Update Dataset"
       
       setwd(wd_json)
       
       #Create json file name from the variable "full_dataset_name".
       
       json_file <- paste(full_dataset_name, "JSON", sep = ".")
       
       #We have to use a little trick in order to be compatible with the python script that 
        #we use to send de JSON-files and update the date of last update of the dataset on 
        #the open data portal.
       #It seems that when we convert to json in R, the values are not strings but instead the 
        #toJSON() function puts square brackets '[]' around the value. Despite the fact
        #that it is still a valid JSON, it is not accepted by the CKAN API.
       #Therefore we apply the following trick:
          #1) We read in the json as a pure string.
          #2) We put the entire json between square brackets '[]'. Doing this results in a CKAN-compatible 
              #json when we apply the toJSON() function in R on that string.
          #3) We read in the string as JSON so that we can access the data in the JSON.
          #4) We update the field "Laatste Update Dataset" in the JSON.
          #5) We convert the JSON back to a string and we remove the square brackets at the beginning
          #   and the end of the string. 
          #6) Finally we write the string to a JSON file.
       
       #1) + 2) + 3) ==>Reading in the JSON as text file and JSON object.
       
       json <- paste(readLines(con = json_file), collapse = " ")
       json <- paste("[", json, "]") #adding the square brackets
       json <- fromJSON(json, simplifyDataFrame = TRUE)
       
       #4) Extract the key "Laatste Update Dataset" and modify its value to the date the dataset was exported.
       #This code subsets the data.frame inside the json containing the "extras" information.
       #We then filter on the key "Laatste Update Dataset" and grab its value 
       #(which is the timestamp the dataset was last updated). So this is the timestamp of the CSV.
       
       #Set working directory to that of the CSV-files because we need to grab the date of the CSV-file
       
       setwd(wd_csv)
       
       #Get the timestamp of the csv-file
       
       #Backup > in case we want to only keep our date instead of the timestamp we have to put this
       #code instead > format(file.mtime(csv_file), "%d-%m-%Y")
       
       csv_timestamp <- as.character(
                                     format(
                                       file.mtime(csv_file), 
                                       "%d-%m-%Y %H:%M:%S"
                                     )
                                   )
       
       
       json$extras[[1]][json$extras[[1]]$key=="Laatste Update Dataset",]$value <- csv_timestamp
       
       #5) Convert back to JSON, convert to a string and remove the square brackets to obtain
           #a JSON-format that is accepted by the API of CKAN (open data portal)
       
       json <- as.character(toJSON(json))
       
       #We use the substring function starting at the second position and ending 
        #at the last but one character.
       
       json <- substr(x =  json, start = 2, stop = nchar(json) - 1)
       
       #Change directory to where JSON files are stored.
       
       setwd(wd_json)
       
       #Write the updated JSON-file.
       
       writeLines(text = json, con = json_file)
       
     }
     
     #Close the database connection to free up memory
     
     dbDisconnect(connection)
 
 #2 - Transfer of CSV to FTP-server####
     
     #Transfer the CSV-files to the FTP server.
     #It was more practical to do this with python.
     #We therefore transfer the files by running the pythonscript "FtpTransfer.py"
        #in this R session
     #In order to run python from R we need a special package "PythonInR"
     
     library("PythonInR")
     
     #Define location of python on the local pc
     
     py_path <- config$python_executable
     
     #Connect python executable with R
     
     pyConnect(py_path)
     
     #Setwd back to scripts
     
     setwd(wd_scripts)
     
     #Execute the python file to transfer the CSV-files
     
     pyExecfile(filename = "Ftp_Transfer.py")
     
     #Setwd back to scripts
     
     setwd(wd_scripts)
 
     
#3 - Update Metadata on the open data portal (CKAN)####
         
     #Adjusting the date of extraction using the python script 
     #"Script_Bulk_Update_MetaData_CKAN.py"
     
     pyExecfile(filename = "Script_Bulk_Update_MetaData_CKAN.py")
    
#4 - Reporting, archiving and sending E-mail####
       
     
     #Archive. We export the data from the JSON-files in a CSV so
        #that it can be imported later in a database.
     #There are two type of files involved: the metadata for publication on
        #the open data portal and the file with the description of the fields
        #in the dataset.
     #We only keep the latest state (so if a dataset was updated earlier,
        #we do not keep al instances of the metadata of that dataset)
     #Creating these archive files is done using the R-script.
     
     #Source the R-script for archiving "Archiving_JSON_To_Data_Table.R"
     
     setwd(wd_scripts)
     
     source("Archiving_JSON_To_Data_Table.R")
     
     #Get some statistics on transfered files for monitoring
      
        #Number of sql-query's that are executed
     
     number_of_sql <- length(sql_files)
     
        #Number of CSV-files on the ftp-server. We get this by accessing the variable 
          #"csvFilesTransfered" in our pythonscript "FtpTransfer.py". We can use the function
          #pyGet() for that.
     
     csv_ftp_server <- pyGet("csv_files_transfered")
     number_csv_ftp_server <- length(csv_ftp_server)
      
     #Sending a mail using our outlook address notifying the job has finished.
      #This code was taken from the following forum:
        #https://stackoverflow.com/questions/26811679/sending-email-in-r-via-outlook
     
     #Create the body of the mail with some stats and the created csv files.
     
    body <- paste(
                 "De onderstaande open datasets en metadata zijn op", 
                 Sys.time(),  
                 "gecreëerd.",
                 "\n\n",
                 "Het aantal uitgevoerde SQL-query's: ",
                 number_of_sql,
                 "\n",
                 "Het aantal CSV-bestanden op de FTP-server: ",
                 number_csv_ftp_server,
                 "\n\n",
                 "De volgende CSV-bestanden werden bijgewerkt: ",
                 "\n\n",
                 paste(csv_ftp_server, collapse = '\n ')
            )
     
    #Send mail
    
     library(RDCOMClient)
     ## init com api
     OutApp <- COMCreate("Outlook.Application")
     ## create an email
     outMail = OutApp$CreateItem(0)
     ## configure  email parameter
     outMail[["To"]] = config$email
     outMail[["subject"]] = paste("JOB_EXTRACT_OPEN_DATA", Sys.time())
     outMail[["body"]] = body
     ## send it
     outMail$Send()
