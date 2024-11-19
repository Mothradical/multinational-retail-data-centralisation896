# Multinational Retail Data Centralisation

A series of Python Classes with methods designed to extract data, clean data, and initialise/upload to a database.

Also contains a series of sample SQL queries to run on the database.

## Description

The first aim of this project was to build Python Classes to extract and clean data, and to upload that data to a database. While the Classes and their methods can be adapted for use on other sources of data, they were designed for a set of data from specific sources.

The second aim of this project was to run SQL queries to build the database schema, and then run a series of queries on the data.

Throughout this project, the primary lesson I learned was "if you don't use it, you lose it". Haven't not coded with Python for a few weeks, the early stages of this project were extremely difficult, as I repeatedly made basic erros in my code and forgot how Python Classes worked. After a while it all started to make sense again.

A second lesson I learned was to never take the easy, albiet time-comsuming way out by cleaning each piece of data that causes an error, and to instead refer to documentation for solutions. For example, I initially began to write a line of code in a cleaning method for every instance of a value that could not be converted to datatime. However, engaging with the documentation for .pd.to_datetime() revealed an error handling parameter, using which I could set all of these instances to NULL values and remove them later with .dropna(). This required more intellectual engagement than simply dealing with each error as it arose, but saved time not just on this method, but nearly every subsequent cleaning method.

## Installation instructions

The python (.py) files each contain a Class. To use them, copy/paste the code into your own Python code, or simply download files.

The Classes, their imports, and their pre-defined variables should be compiled into a single document before use, as all will need to be utilised for a successful upload.

You may be missing some Python packages needed to run this code. If so, your error code should inform you which, and a quick pip install <python_package> should rectify the issue.

queries.sql contains the queries ran as part of this project and can simply be downloaded, no installation necessary.

## Usage instructions

After compiling the Classes, create objects that contain instances of the classes (i.e. dbcon = DatabaseConnector()). Now you have access to the Class methods through the objects. These can be used to extract data from the sources defined in the variables, clean that data, and upload it to a database. You'll have to change variables for your own purposes; the upload_path and localpath (where you store a yaml file containing database credentials) will most likely be different per user.

Before running the queries, alterations on the Database are necessary:
1. Bunk columns (e.g. index) need to be dropped.
2. Columns need their appropriate datatype.
3. The 'month' column of the date table needs to be 2-digits (e.g. 1 needs to be changed to 01).
4. Primary and foreign keys need to be set, with the orders table acting as the Bridge/Association Table.
5. Other changes were made to my personal database as part of this project, but none of these are required to run the queries.

Following this, the queries in queries.sql should run without issue.

## File structure of the project

database_utils.py: contains the class DatabaseConnector
data_cleaning.py: contains the class DataCleaning
data_extraction.py: contains the class DataExtractor
queries.sql: contains SQL queries

## License information
n/a

