
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	#Create dataset on productionserver
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#Import the required modules

from ckanapi import RemoteCKAN
import pandas
import os

  #Make a connection to the API using the RemoteCKAN function from the
  #ckanapi module.

myAddress = input("URL open data: ")
myApiKey = input("API-key: ")
metaDataDirectory = input("Locatie metadata bestand: ")

 
mySite  = RemoteCKAN(
		address = myAddress,
         apikey = myApiKey
           )

#Read in json file from desktop

	#Change directory where the metadata resides
	
os.chdir(metaDataDirectory)		   
		   
myMetaData = pandas.read_json('Metadata.JSON', typ = 'series')

	#Convert to dictionary

myMetaData = myMetaData.to_dict()

	#Remove the key 'VersionForm' because we only need this for archiving and not for 
	#publishing on the open data portal.

del myMetaData['VersionForm']	

	#Publish to open data portal
	
mySite.call_action(action = 'package_create', data_dict = myMetaData)

