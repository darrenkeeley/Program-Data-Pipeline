# Next Best Action pipeline

## Usage
This program downloads data on program enrollment and eligibility requirements and uploads it to a secure file transfer (SFT) site for a vendor to analyze. 

See Setup section for how to use the program.

## Included Programs
* Budget Billing
* Paperless Billing
* Recurring Payment

## Requirements
* Python 3, 64 bit
* WinSCP
* Oracle ODBC driver, 64 bit

## Setup
* To run the program, execute PieToSFT.py, which iterates through the Queries folder, querying the databases and saving the results locally as tab delim txt. Once all the data is downloaded, the script calls RunWinscpScript.bat which calls WinSCP to upload the files to SFT.

* Before running, update config.xml with login credentials and change filepaths to match your system and location of WinSCP.

* Create ODBC connections for the databases by going to Start > OBC Data Source Administrator (64-bit). Add an Oracle ODBC for each, with the names "td", "cas" and "crcr". Example TNS for td:

```
TDPRD=
 (Description=
  (Address=
   (PROTOCOL=TCP)
   (HOST=company.com)
   (PORT=1234)
  )
   (CONNECT_DATA=
    (SERVER=DEDICATED)
	(SERVICE_NAME=company.prod.com)
   )
 )
```

* Queries can be added or removed from the Queries folder. The folder structure is Program Name / Database Name / SQL query that is a SQL file named after the data.
