from ftplib import FTP
import os 
import pandas

#Create an ftp connection with the ftp open data webserver

#read in parameters from config file

config_py = pandas.read_json("config.json", typ='series')

#Store the login data in variables

ftp_server = config_py['ftp_server']
user_name = config_py['ftp_user_name']
password = config_py['ftp_password']

#Create the connection

ftp = FTP(ftp_server)  
ftp.login(user=user_name, passwd=password)  

#collect the filenames of the files to be transferred 

	#Change directory to local directory of CSV files
	
os.chdir(config_py['wd_csv'])	

	#Capture all csv-files
csv_files = os.listdir()

	#Navigate to directory on ftp-server
	
ftp.cwd(config_py['ftp_csv_directory'])

for i in csv_files:
	ftp.storbinary('STOR '+i, open(i,'rb'))

#Get the number of csv_files on the ftp server

csv_files_transfered = ftp.nlst()
csv_files_transfered = [x for x in csv_files_transfered if x.endswith(".csv")]
	
ftp.quit()
