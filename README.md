## Table of contents
* [Project Summary](#Project-Summary)
* [How to Run the Project](#How-to-Run-the-Project)
* [Description of Repository](#Description-of-Repository)
* [Purpose of the Postgres Database](#Purpose-of-the-Postgres-Database)
* [Database Schema Design](#Database-Schema-Design)
* [ETL Pipeline Design](#ETL-Pipeline-Design)
* [References](#References)

## Project Summary
Sparkify is in need of a relational database that can house all of their song play data for analytic purposes. They will also need an ETL process that will take their data in the form of JSON files and load that into the relational database in Postgres. 

## How to Run the Project
1. Open the Launcher
2. Start Terminal
3. Run "python create_tables.py"
4. Run "python etl.py"
5. Open test.ipynb and run the select statements to confirm data has been loaded into the 5 tables

## Description of Repository
The repository contains the following data as JSON files:
1. Song Data - A listing of songs that Sparkify has.
2. Log Data - A log of what songs users have been playing in the Sparkify application.

## Purpose of the Postgres Database
The data model will allow Sparkify to query and analyze the song plays from their application. This would provide Sparkify with valuable information such as which songs are being played, how long, and where. The data model would also tell Sparkify about their users and their preferences in music or if they're using a paid subscription. 

## Database Schema Design
For the schema design, I tried to keep it as simple as possible and to prevent any datatype conflicts with the ETL pipeline. Varchar was chosen for the text fields, time for the time fields, and int/numeric for the numeric fields. Only 1 numeric data type was needed for duration. As for the lattitude and longitude data, Float was used since it's known to be pretty good for coordinate data. 

## ETL Pipeline Design
For the ETL process, I started off by using pandas to read the json files and threw the result into a data frame. That allowed me to easily transform and load the data. When loading the time table, I had to create two lists for time data and column labels. Then I had to combine them into a data frame. I found a great code example (listed under references) that used a list comprehension technique to accomplish this for a dictionary. Once I had the data in the dictionary, I was able to convert it to a data frame using pandas. 

## References
- Converting two lists into a data frame: https://www.tutorialspoint.com/How-to-create-Python-dictionary-from-list-of-keys-and-values
