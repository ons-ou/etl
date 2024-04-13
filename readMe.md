# Air Quality ETL Project

This project focuses on the ETL (Extract, Transform, Load) process to extract pollution data from 
the EPA's Outdoor Air Quality Data website['https://www.epa.gov/outdoor-air-quality-data'].

## Packages
### Data Sources
There are two current data sources for this project:

#### ZIPDownloader
This downloads the pre-packaged data from ['https://aqs.epa.gov/aqsweb/airdata/download_files.html']
Each zip file contains all the data for one element and one year. While updated regularly, the data is still behind 
in comparison (At the moment last update is October 2023)

#### SeleniumDownloader
This downloads the newer data. The problem is csv files need to be downloaded per state and per year
which takes a lot of time.

N.B: Data from this source is missing the arithmetic mean and first max hour values.
Reference: 'https://www.youtube.com/watch?v=PjDQ_MIL8JI'

### etl
This package defines the functions for Extracting, Transforming, Merging,
and Loading the air quality data. It handles the processing of the 
downloaded CSV files.

####BaseWorkflow
This contains the code for:
- initializing workflow: Create tables and indexes if not already available and get the last date that is already saved in database.
- loading data into database
- starting multiple threads for all elements and years

####ZipWorkflow
This contains the code of:
- data transformation of zip files
- data extraction of zip files
    Steps of workflow:
    1. Gets the last update of this site [https://aqs.epa.gov/aqsweb/airdata/download_files.html] and compares it to the cached last update
    2. If they are both the same stops workflow 
    3. If not, gets the last saved date in the element table (if no table exists then uses STAR_DATE in etl_utils) and starts extraction fr
    that date upto LAST_ZIP_DATE (Also in utils extracted from site: year of first row of table in site)
    4. Transforms and loads data into database
    5. Updates cache with the new last site update
    
####SeleniumWorkflow
This contains the code of:
- data transformation of csv selenium files
- data extraction of csv selenium files

###utils
####Database:
This contains the code of:
- database connection
- creating tables and inserting data
- creating indexes
- select max_date from table

####Table Columns
- the columns of the aqi and elements table
P.S: in case of changing the structure of columns, make sure to change the columns_to_keep 
in data transformation of both Workflows

####etl_utils
- The code for the basic data transformation. This is where you should add any changes to the structure of 
the dataframe

####states_and_counties_tables
- This code should be run first to initialize the state and county tables


##Running the project
1. Run pip install -r requirements.txt
2. Run the main.py in states_and_counties_tables
3. Change the constants in etl_utils based on your needs (The start and end date of the data you're getting as well as states and elements)
P.S: states is only needed for selenium workflow
4. Run the main.py in etl.main.py
P.S: There is no main.py for the selenium workflow at the moment due to its slow speed


##TODO
1. Change the database connection into a hosted database (AWS)
2. Figure out a way to speed up downloads for the selenium csv files
PS:
- if you wish to change structure of tables must change both columns in utils and columns to keep of data transformation function
- if you want to add any additional cleaning or transformations do it in the etl_utils function