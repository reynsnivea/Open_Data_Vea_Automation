from ckanapi import RemoteCKAN
import pandas
import os

# Connect to CKAN

#myAddress = input("URL open data: ")
#myApiKey = input("API-key: ")
#wd_packages = input("Locatie metadata bestanden: ")

#read in parameters from config file

config_py = pandas.read_json("config.json", typ='series')

#set parameters

my_address = config_py["ckan_url"]
my_api_key = config_py["ckan_api_key"]
wd_packages = config_py["wd_metadata"]

#Remove the config_py object

#del config_py

# Create connection object

my_site = RemoteCKAN(address=my_address,
                    apikey=my_api_key
                    )

#Update datasets to the portal

    # Set working directory to the directory containing the JSON-files
    # with the metadata for the packages.

os.chdir(wd_packages)

    # Check working directory

os.getcwd()

    # Read in a json-file containing the metadata for the dataset.
    # Note that a package = a dataset.
    # A distribution (eg. CSV , XML, etc. of that dataset is a resource)

    # Get all files in the folder
json_files_packages = os.listdir()

    # Extract json-files using a list comprehension

json_files_packages = [x for x in json_files_packages if x.endswith(".JSON")]

    # Update the datasets (packages) on the open data portal

    #Loop through the files

for x in json_files_packages:
    meta_data = pandas.read_json(x, typ='series')

    # Remove the key 'VersionForm' because we only need this for archiving and not for
    # publishing on the open data portal.

    del meta_data['VersionForm']

    # Convert the json-file we read in to a python dictionary.
    # When sending data in a request we need to send it as a python dictionary

    meta_data = meta_data.to_dict()

    # Use the method call_action() on object mySite (= the connection string)
    # We provide 2 parameters: the API-method we use and the metadata we want to send.

    my_site.call_action(action='package_patch', data_dict=meta_data)

