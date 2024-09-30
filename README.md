# COVID-19 Data Automation and Analysis Project

## Description

This project automates the daily ETL (Extract, Transform, Load) process for COVID-19 data using Python, MySQL, Google Sheets, and Power BI. Data is extracted from [Our World in Data](https://ourworldindata.org/coronavirus-source-data), transformed, and loaded into a MySQL database. It is then integrated with Google Sheets for real-time updates and visualized in Power BI for dynamic insights.

## About the Dataset

The COVID-19 dataset is sourced from [Our World in Data](https://covid.ourworldindata.org/data/owid-covid-data.csv), providing comprehensive and up-to-date information on various COVID-19 metrics across different countries and regions.

> **Key attributes include**:
>
> - **General Information**:
>   - `iso_code`, `continent`, `location`, `date`
> - **Cases and Deaths**:
>   - `total_cases`, `new_cases`, `total_deaths`, `new_deaths`
> - **Testing and Vaccinations**:
>   - `total_tests`, `new_tests`, `positive_rate`
>   - `total_vaccinations`, `people_vaccinated`, `people_fully_vaccinated`
> - **Hospitalization**:
>   - `icu_patients`, `hosp_patients`
> - **Other Metrics**:
>   - `reproduction_rate`, `stringency_index`, `population`
> - **Demographics and Socioeconomic Indicators**:
>   - `median_age`, `gdp_per_capita`, `life_expectancy`, `human_development_index`

## Project Structure

1. **`README.md`**: Contains essential information about the dataset and project orientation.
2. **`automation(final).py`**: Main Python script automating the ETL process:
   - Downloads and processes data.
   - Loads data into MySQL.
   - Executes SQL queries for analysis.
   - Updates Google Sheets with results.
   - Logs execution details.
3. **`update_log.txt`**: Logs timestamps and runtimes of each execution.
4. **`Covid Tracking Dashboard (realtime)`**: Contains Power BI dashboard files for data visualization.

## Project Highlights

- **Automated ETL Process**: Ensured up-to-date data by automating extraction, transformation, and loading into MySQL.
- **Reliable Data Retrieval**: Implemented robust downloading with error handling and retries.
- **Efficient Data Processing**: Used pandas to focus on deaths and vaccinations for targeted analysis.
- **Database Integration and Analysis**: Loaded data into MySQL and executed SQL queries for insights on global totals, location-specific data, and death rates.
- **Real-Time Updates**: Updated Google Sheets using `pygsheets` for immediate data sharing.
- **Interactive Visualization**: Developed a Power BI dashboard connected to live data sources.
- **Execution Logging**: Recorded update timestamps and runtimes for monitoring.

## Tools/Languages/Libraries Used

- **Programming Language**:
  - Python, SQL

- **Python Libraries**:
  - `pandas`, `requests`, `pyodbc`, `pygsheets`, `datetime`, `time`

- **Database**:
  - MySQL

- **Visualization Tools**:
  - Power BI

- **Others**:
  - Google Sheets
  - Our World in Data COVID-19 Dataset

## Getting Started

To replicate or run this project:

1. **Prerequisites**:
   - Install Python 3.x.
   - Install required libraries:
     ```bash
     pip install pandas requests pyodbc pygsheets
     ```
   - Set up a MySQL database.
   - Obtain Google API credentials for `pygsheets`.

2. **Configuration**:
   - Update database connection details in `automation(final).py`:
     ```python
     conn = pyodbc.connect('Driver={MySQL ODBC 8.4 Unicode Driver};'
                           'Server=YOUR_SERVER;'
                           'Port=YOUR_PORT;'
                           'Database=YOUR_DATABASE;'
                           'UID=YOUR_USERNAME;'
                           'PWD=YOUR_PASSWORD;'
                           'OPT_LOCAL_INFILE=1;')
     ```
   - Specify the path to your Google API credentials:
     ```python
     creds = 'path/to/your/serviceaccountapikey.json'
     ```

3. **Execution**:
   - Run the script:
     ```bash
     python automation(final).py
     ```
   - The script will perform the ETL process, update Google Sheets, and log details.

4. **Power BI Dashboard**:
   - Open the dashboard in `Covid Tracking Dashboard (realtime)`.
   - Ensure data sources connect to your MySQL database and Google Sheets.

## Contact Information

For questions or further information, please contact me via [Duc Minh (David)](www.linkedin.com/in/duc-minh-ngn)