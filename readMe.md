# Air Quality ETL Project

This project focuses on the ETL (Extract, Transform, Load) process to extract pollution data from 
the EPA's Outdoor Air Quality Data website['https://www.epa.gov/outdoor-air-quality-data/download-daily-data'].

## Files
### EPAAirQualityDownloader
This file uses Selenium to download the CSV file containing air quality
data from the EPA website.

Reference: 'https://www.youtube.com/watch?v=PjDQ_MIL8JI'

### etl
This file defines the functions for Extracting, Transforming, Merging,
and Loading the air quality data. It handles the processing of the 
downloaded CSV files.

### main
The main file allows running the ETL process in parallel, optimizing 
the data processing speed.

##TODO
The final goal is a completely streamlined process where on run new data is extracted, transformed and
added to database. The job should only download data for years not already in database.

1. At the moment, the selenium code requires many sleeps to actually work and even then it would 
fail if running same function parallely. Try to fix it.
2. Add the code for data cleaning and transformation. Final expected result should be similar to this
notebook['https://colab.research.google.com/drive/1D0IJF2Ew20Hz6H03ONNGw7GyDLDJLABC?usp=sharing']
PS. The co_daily_summary already has quite a few transformations. If someone has the code for that add it.
3. Choose a database to load the data to.
4. Write the code for the job where you can either extract all data or only new data not in database.
