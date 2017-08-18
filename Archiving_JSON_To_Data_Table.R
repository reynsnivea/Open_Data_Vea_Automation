

#Create a wrapper function in which we wrap our sript
  #We do this because we do not want any variables to be returned in our global
  #environment when we call this script from another R-script.

archive <- function(){


##########################################################################
#+++In this script, we import the JSON-data that is generated in the+++# 
#+++registration form for the open datasets+++#
##########################################################################

#0. Load required packages----

  #Load required packages
  
  library(jsonlite)
  library(data.table)


#1. Retreive JSON-files for the datasets----
  
  #We have two kind of files that come from the registration form for the 
    #open datasets
  #Fielddata ==> These JSON-files contain field data i.e. the description of
    #the fields in the open dataset 
  #Metadata ==>These files contain the meta data on the registered datasets.
  
  #Import the metadata file and the field data files:
  
  #Set default working directory
  
  # main_wd <- readline(prompt="Locatie bronmap met alle datasets: ")
  # source_wd <- readline(prompt="Locatie doelmap CSV met alle metadata: ")

  wd_metadata  <-  config$wd_metadata
  wd_fielddata <-  config$wd_fielddata
  wd_log       <-  config$wd_log
  
  
  #Get all JSON-files. Those files contain the metadata and the data description of the 
    #fields in the dataset.
    #We use the list.files() function in order to retreive all relevant files in all subdirectories
      #(recursive = TRUE)
  
  setwd(wd_fielddata)
  
  files_fielddata <- list.files(pattern = ".JSON", recursive = TRUE)
  
  #We store the number of files in "files_fielddata" because we need it later.
  
  nof_fielddata <- length(files_fielddata)
  
  #We filter the name of the dataset from the filepath. We do this by searching the first occurence of
    #"/" in the file path.
  
  position_slash <- regexpr(text = files_fielddata, pattern = ".JSON")
  
  #Get datasetnames using substring method. we go from the first character until the position_slash - 1.
  
  dataset_names <- substr(x = files_fielddata, start = 1, stop = position_slash - 1)

  #Now we search for the metadata.JSON files and store them also in a vector.
  
  setwd(wd_metadata)
  
  files_metadata <- list.files()
  files_metadata <- files_metadata[grepl(".JSON", files_metadata)]

#2. Loop through the files and store the data in a data.table----

  #Loop the fielddata files

  #Create object FieldData
  
  FieldData <- NULL
  
  #Loop through the different FieldData files
  
  index <- 1 #We need the index to retreive the correct datasetname from dataset_names.
  
  for (i in files_fielddata){ #i is a path to a filename here
    
    setwd(wd_fielddata) #reset to main_wd so that we can import Metadata.JSON for the i-th file.
    FieldData[[index]] <- fromJSON(txt = i, simplifyDataFrame = TRUE)
    FieldData[[index]]$DatasetName <- dataset_names[index]
    index <- index + 1
  }
 
  #Bind the list items together to one data.table.
  
  FieldData <- rbindlist(FieldData)
  
  #Move the datasetName variable to the second column.
  
  setcolorder(FieldData, c(1,5,2:4))

  #Loop through the Metadata files
  
  setwd(wd_metadata)
  
  #Create object MetaData
  
  MetaData <- NULL
  
  #Note that we don't need to add the datasetName to the result here
    #because the Metadata.JSON files already have the DatasetName.
  
  for (i in files_metadata){ #i is a path to a filename here
    
    setwd(wd_metadata) #reset to main_wd so that we can import Metadata.JSON for the i-th directory.
    setwd(wd_metadata)
    MetaData[[i]] <- fromJSON(txt = i, simplifyDataFrame = TRUE)

  }

  #Bind the list items together to one data table.
  
  MetaData <- rbind(MetaData)

  #Export to a csv
  
  setwd(wd_log)
  
  
  #Before we can export the metadata in a reasonable manner, we have 
    #to rearrange the list and turn it into a data frame.
  #In order to achieve this we use the unlist() function but apparently
    #this only returns the values and not the fieldnames.
  #But when we use the cbind() function with it, we can collect the fieldnames.
  
  export_metadata <- NULL
  
  for(i in 1:length(MetaData)){
    
    export_metadata[[i]] <- as.data.frame(
                              cbind(
                                  json = colnames(MetaData)[i],
                                  value = unlist(MetaData[[i]])
                                )
                              )
    export_metadata[[i]]$key <- row.names(export_metadata[[i]])
    
 }
  
  
  export_metadata <- rbindlist(export_metadata)
  
  export_metadata <- as.data.table(export_metadata)
  
  setcolorder(export_metadata, c(1,3,2))
  
  write.csv(x = export_metadata, file = "Archive_MetaData.csv", row.names = FALSE, quote = TRUE)
  write.csv(x= FieldData, file = "Archive_FieldData.csv", row.names = FALSE, quote = TRUE)
  
  #Message that sript has run
  
  print("Het script is beëindigd !")
  
}

#Call the archive function

archive()  
  