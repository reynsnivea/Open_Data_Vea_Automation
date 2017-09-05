# Open_Data_Vea_Automation

In this README we document the files related to the automation of the 
open data extraction and publication process. 
The process involves the extraction of open datasets from databases, modifying
the metadata of the dataset an publishing the datasets to the open data portal.

This is achieved by several scripts in R and python.

This README has two sections:

* Section 1 describing the creation of this git repository.
* Section 2 describing the files in the automation of the open data process.

## 1. Build Git Repository

### 1.1 Create repository

``` 
git init
```

### 1.2 Create .gitignore text file.

	*Add config.json to this file
	*Add the folder Backup_V1 to gitigore

### 1.3 Add all files to the staging area

```
git add .
```

### 1.4 Commit first working production version on het "master" branch

```
git commit . -m "first working production version"
```

### 1.5 Create repository on GitHub with repository name: "Open_Data_Vea_Automation"

```
git remote add origin https://github.com/reynsnivea/Open_Data_Vea_Automation.git
git push -u origin master
```

### 1.5 Now create a testing branch "testing".
* We will use this branch to modify our scripts if necessary.
* This is done by modifying our scripts by using the test parameters from our configuration file
* First we test a commit by only modifying one parameter and commit it to our testing branch.
* We then verify if our code in production in still intact

```
git commit . -m "first test commit"
```

* Commit succesful.
* We now modify all our parameters to the testing parameters.

## 2. Files Related To The Project

The automation is realized using serveral files.
We wanted to use the R language as much as possible to use the data.table 
data structure as much as possible. 
For some tasks, we believed Python was the appropriate language.

In the sections below we describe the briefly describe the functionality of each 
script.

The automation process requires that the dataset is already created on the open
data portal. Automation only relates to the update process of the datasets. 

### 2.1 JOB_EXPORT_OPEN_DATASETS.R

This script is the main script which calls other scripts.
The main script connects to the database and runs queries against it.
It stores the data as a CSV-file. Once data is extracted, the date of extraction
in the metadata of the dataset (JSON-format) is updated. 
The data is transfered using FTP and the metadata is updated by calling the API
of the CKAN open data portal.

### 2.2 Ftp_Transfer.py

Transfering files to the FTP-server is achieved by this python script.
A Connection is made to the server and the CSV-files are transfered to the 
server. This script is called from script JOB_EXPORT_OPEN_DATASETS.R.

### 2.3 Archiving_JSON_To_Data_Table.R

All metadata and fielddata related to the datasets is archieved in 2 CSV-files:

* Archive_FieldData.csv
* Archive_MetaData.csv

We do not keep any history. The files reflect the state after the last update.

### 2.4 Create_Dataset_CKAN.py

Creating a new dataset on the open data portal is achieved by this script. 
It stands aside from the automation process as it is used prior to the 
update process.

### 2.5 Script_Bulk_Update_MetaData_CKAN.py

Updating the metadata on the open data portal is realized by calling this script 
from the script JOB_EXPORT_OPEN_DATASETS.R.
It loops over all metadatasets and sends the metadata in JSON-format over the
API of the open data portal.



