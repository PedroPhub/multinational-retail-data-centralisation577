# Multinational retail data centralisation

## Overview

This repository houses the code and resources necessary for extracting, cleaning, and storing data from various sources into a target database. It's designed to streamline the process of data integration and ensure data quality for subsequent analysis and use.

## Key Features

- Flexible Data Source Support: Handles multiple data sources, including:
    Flat files (CSV, JSON, PDF)
    Databases (PostgreSQL, AWS RDS)
    APIs
    Web scraping (S3 bucket)
- Comprehensive Data Cleaning: Implements data cleaning techniques to address:
    Missing values
    Inconsistent formatting
    Data type errors
    Outliers
- Database Storage: Stores cleaned data in a designated database.

## Installation

1. Prerequisites:
    - yaml
    - sqlalchemy
    - pandas
    - boto3
    - tabula
    - dateutil.parser


## File Structure

[Multinational Retail Data Centralisation]

- [main.py]
- [data_handling]
   - [data_cleaning.py]
   - [data_extraction.py]
   - [database_utils.py]
- [SQL_database_schema]
   - [star_based_schema.sql]
- [data_querying]
   - [data_metrics.sql]
- [README.md]

### Class DataCleaning (data_cleaning.py)
This class contains methods to clean data from each of the data sources.

### Class DataExtractor (data_extraction.py)
This class will work as a utility class, containing methods that help extract data from different data sources.

### Class DatabaseConnector (database_utils.py)
This class will connect with and upload data to databases.

### SQL_database_schema folder
Contains SQL statements that establishes a star-based schema of the database, ensuring that the columns are of the correct data types.

### data_querying folder
Contains SQL statements that extract data metrics.
