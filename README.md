# Multinational Retail Data Centralisation

A series of Python Classes with methods designed to extract data, clean data, and initialise/upload to a database.

Also contains 2 sets of SQL queries, one to transform the tables and the other a sample of queries to run on the database.

## Description

The first aim of this project was to build Python Classes to extract and clean data, and to upload that data to a database. While the Classes and their methods can be adapted for use on other sources of data, they were designed for a set of data from specific sources.

The second aim of this project was to run SQL queries to build the database schema, and then run a series of queries on the data.

Throughout this project, the primary lesson I learned was "if you don't use it, you lose it". Haven't not coded with Python for a few weeks, the early stages of this project were extremely difficult, as I repeatedly made basic erros in my code and forgot how Python Classes worked. After a while it all started to make sense again.

A second lesson I learned was to never take the easy, albiet time-comsuming way out by cleaning each piece of data that causes an error, and to instead refer to documentation for solutions. For example, I initially began to write a line of code in a cleaning method for every instance of a value that could not be converted to datatime. However, engaging with the documentation for .pd.to_datetime() revealed an error handling parameter, using which I could set all of these instances to NULL values and remove them later with .dropna(). This required more intellectual engagement than simply dealing with each error as it arose, but saved time not just on this method, but nearly every subsequent cleaning method.

## Installation instructions

main.py contains all the Classes compiled into a single document, while the other .py files contain the code for each Class.

You may be missing some Python packages needed to run this code. If so, your error code should inform you which, and a quick pip install <python_package> should rectify the issue.

table_formatting.sql contains the code required for table transformations.

queries.sql contains the queries ran as part of this project and can simply be downloaded, no installation necessary.

## Usage instructions

The file main.py contains all the classes and instances of the classes at the end. These can be used to extract data from the sources defined in the variables, clean that data, and upload it to a database. You'll have to change variables for your own purposes; the localpath and sec_det_pth (where you store a yaml file containing database and security credentials) will be different per user.

After cleaning and uploading the database tables, run the table_formatting.sql code to perform the necessary table transformations.

Following this, the queries in queries.sql should run without issue.

## File structure of the project

database_utils.py: contains the class DatabaseConnector

data_cleaning.py: contains the class DataCleaning

data_extraction.py: contains the class DataExtractor

main.py: contains the compiled classes

table_formatting.sql: contains SQL code for formatting the tables

queries.sql: contains SQL queries

.gitignore: contains files not push to github (i.e. security and database details)

sales_data_STAR.png: contains visualisation of star-based schema

README.md: The file you're currently reading

## License information
n/a

