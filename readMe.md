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
2. Parallel threads take too much time to work, use airflow instead (I have started the code someone work on it)
3. Use AWS RDS instead of local database
