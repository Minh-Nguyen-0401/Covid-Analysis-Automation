
import pandas as pd

from datetime import datetime
start_time = datetime.now()

pd.set_option('float_format', '{:}'.format)

import requests
import pandas as pd
from time import sleep


def download_file(url, file_path, max_retries=5, backoff_factor=1):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            block_size = 8192  # 8 Kibibytes

            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=block_size):
                    if chunk:
                        file.write(chunk)
                        downloaded_size += len(chunk)
                        progress_percentage = (downloaded_size / total_size) * 100
                        print(f"\rDownloading: {progress_percentage:.2f}%", end='')
            
            print("\nDownload successful")
            return file_path
        
        except (requests.exceptions.RequestException, IOError) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                sleep(backoff_factor * (2 ** attempt))  # Exponential backoff
            else:
                raise

file_path = 'owid-covid-data.csv'
url = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'

print("Downloading data...")
download_file(url, file_path)

print("Exporting data to local csv file")
covid_data = pd.read_csv(file_path)
print(covid_data)


covid_deaths = pd.concat([covid_data.iloc[:,:4],pd.DataFrame(covid_data['population']),covid_data.iloc[:,4:25]], axis = 1)
covid_vaccinations = pd.concat([covid_data.iloc[:,:4],covid_data.iloc[:,25:44],covid_data.iloc[:,45:]],axis =1)

# Save them as csv
covid_deaths.to_csv(r"D:\Study\UNIVERSITY\OTHER COURSES\random coding\Portfolio Projects\Covid Analysis Automation\processed dataset\covid_deaths.csv",index = False)
covid_vaccinations.to_csv(r"D:\Study\UNIVERSITY\OTHER COURSES\random coding\Portfolio Projects\Covid Analysis Automation\processed dataset\covid_vaccinations.csv",index = False)


import pyodbc 
conn = pyodbc.connect('Driver={MySQL ODBC 8.4 Unicode Driver};'
                      'Server=127.0.0.1;'
                      'Port=3307;'
                      'Database=covid_analysis_project;'
                      'UID=root;'
                      'OPT_LOCAL_INFILE=1;') 
cursor = conn.cursor()

# drop_table = '''
# DROP TABLE IF EXISTS `covid_data`;
# '''

# create_table = '''
# CREATE TABLE `covid_data` (
#   `iso_code` VARCHAR(255) DEFAULT NULL,
#   `continent` VARCHAR(255) DEFAULT NULL,
#   `location` VARCHAR(255) DEFAULT NULL,
#   `date` DATE DEFAULT NULL,
#   `total_cases` FLOAT DEFAULT NULL,
#   `new_cases` INT DEFAULT NULL,
#   `new_cases_smoothed` FLOAT DEFAULT NULL,
#   `total_deaths` FLOAT DEFAULT NULL,
#   `new_deaths` INT DEFAULT NULL,
#   `new_deaths_smoothed` FLOAT DEFAULT NULL,
#   `total_cases_per_million` FLOAT DEFAULT NULL,
#   `new_cases_per_million` INT DEFAULT NULL,
#   `new_cases_smoothed_per_million` FLOAT DEFAULT NULL,
#   `total_deaths_per_million` FLOAT DEFAULT NULL,
#   `new_deaths_per_million` INT DEFAULT NULL,
#   `new_deaths_smoothed_per_million` FLOAT DEFAULT NULL,
#   `reproduction_rate` FLOAT DEFAULT NULL,
#   `icu_patients` FLOAT DEFAULT NULL,
#   `icu_patients_per_million` FLOAT DEFAULT NULL,
#   `hosp_patients` FLOAT DEFAULT NULL,
#   `hosp_patients_per_million` FLOAT DEFAULT NULL,
#   `weekly_icu_admissions` FLOAT DEFAULT NULL,
#   `weekly_icu_admissions_per_million` FLOAT DEFAULT NULL,
#   `weekly_hosp_admissions` FLOAT DEFAULT NULL,
#   `weekly_hosp_admissions_per_million` FLOAT DEFAULT NULL,
#   `total_tests` FLOAT DEFAULT NULL,
#   `new_tests` FLOAT DEFAULT NULL,
#   `total_tests_per_thousand` FLOAT DEFAULT NULL,
#   `new_tests_per_thousand` FLOAT DEFAULT NULL,
#   `new_tests_smoothed` FLOAT DEFAULT NULL,
#   `new_tests_smoothed_per_thousand` FLOAT DEFAULT NULL,
#   `positive_rate` FLOAT DEFAULT NULL,
#   `tests_per_case` FLOAT DEFAULT NULL,
#   `tests_units` VARCHAR(255) DEFAULT NULL,
#   `total_vaccinations` FLOAT DEFAULT NULL,
#   `people_vaccinated` FLOAT DEFAULT NULL,
#   `people_fully_vaccinated` FLOAT DEFAULT NULL,
#   `total_boosters` FLOAT DEFAULT NULL,
#   `new_vaccinations` FLOAT DEFAULT NULL,
#   `new_vaccinations_smoothed` FLOAT DEFAULT NULL,
#   `total_vaccinations_per_hundred` FLOAT DEFAULT NULL,
#   `people_vaccinated_per_hundred` FLOAT DEFAULT NULL,
#   `people_fully_vaccinated_per_hundred` FLOAT DEFAULT NULL,
#   `total_boosters_per_hundred` FLOAT DEFAULT NULL,
#   `new_vaccinations_smoothed_per_million` FLOAT DEFAULT NULL,
#   `new_people_vaccinated_smoothed` FLOAT DEFAULT NULL,
#   `new_people_vaccinated_smoothed_per_hundred` FLOAT DEFAULT NULL,
#   `stringency_index` INT DEFAULT NULL,
#   `population_density` FLOAT DEFAULT NULL,
#   `median_age` FLOAT DEFAULT NULL,
#   `aged_65_older` FLOAT DEFAULT NULL,
#   `aged_70_older` FLOAT DEFAULT NULL,
#   `gdp_per_capita` FLOAT DEFAULT NULL,
#   `extreme_poverty` FLOAT DEFAULT NULL,
#   `cardiovasc_death_rate` FLOAT DEFAULT NULL,
#   `diabetes_prevalence` FLOAT DEFAULT NULL,
#   `female_smokers` FLOAT DEFAULT NULL,
#   `male_smokers` FLOAT DEFAULT NULL,
#   `handwashing_facilities` FLOAT DEFAULT NULL,
#   `hospital_beds_per_thousand` FLOAT DEFAULT NULL,
#   `life_expectancy` FLOAT DEFAULT NULL,
#   `human_development_index` FLOAT DEFAULT NULL,
#   `population` INT DEFAULT NULL,
#   `excess_mortality_cumulative_absolute` FLOAT DEFAULT NULL,
#   `excess_mortality_cumulative` FLOAT DEFAULT NULL,
#   `excess_mortality` FLOAT DEFAULT NULL,
#   `excess_mortality_cumulative_per_million` FLOAT DEFAULT NULL
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
# '''

truncate_table = '''TRUNCATE TABLE covid_data;'''

load_data = '''
LOAD DATA INFILE 'D:/Study/UNIVERSITY/OTHER COURSES/random coding/Portfolio Projects/Covid Analysis Automation/owid-covid-data.csv'
INTO TABLE covid_data 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;
'''

try:
    # # Drop existing table if it exists
    # cursor.execute(drop_table)
    # conn.commit()  # Commit table drop
    # print("1")
    
    # # Create the table
    # cursor.execute(create_table)
    # conn.commit()  # Commit table creation
    # print("2")

    # Truncate the table
    cursor.execute(truncate_table)
    conn.commit()  # Commit table truncation
    print("1")

    # Load data into the table
    cursor.execute(load_data)
    conn.commit()  # Commit data loading
    print("2")

except Exception as e:
    print(f"An error occurred: {e}")
    conn.rollback()  # Rollback changes in case of error


query1 = '''
SELECT 
    SUM(new_cases) AS total_cases, 
    SUM(new_deaths) AS total_deaths, 
    SUM(new_deaths)/SUM(new_cases)*100 AS total_death_percentage 
FROM covid_data
WHERE continent IS NOT NULL 
ORDER BY 1,2;
'''

table1 = pd.read_sql_query(query1,conn) 
table1

query2 = '''
SELECT 
    location,
    population,
    date,
    total_cases,
    total_cases/population*100 as infection_rate,
    new_cases
FROM covid_data
WHERE continent IS NOT NULL
AND LOWER(location) like '%vietnam%'
ORDER BY date DESC;
    
'''

table2 = pd.read_sql_query(query2,conn) 
table2

query3 = '''
WITH a as(
SELECT 
    location,
    max(total_cases) as total_case_count,
    max(total_deaths) as total_death_count,
    max(total_deaths)/max(total_cases) as death_rate 
FROM covid_data
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY death_rate DESC)
SELECT * FROM a
WHERE death_rate is not null;
'''

table3 = pd.read_sql_query(query3,conn) 
table3

# Connect to Google Sheets using pygsheets library with my credentials
import pygsheets
creds = r'D:\Study\UNIVERSITY\OTHER COURSES\random coding\Portfolio Projects\API KEY CREDENTIALS\serviceaccountapikey.json'
api = pygsheets.authorize(service_file=creds)

# Open the workbook that contains the final output
wb = api.open('Covid Tables')

sheet1 = wb.worksheet_by_title(f'Sheet1')
sheet1.set_dataframe(table1,(1,1))
print("Done")

sheet2 = wb.worksheet_by_title(f'Sheet2')
sheet2.set_dataframe(table2,(1,1))
print("Done")


sheet3 = wb.worksheet_by_title(f'Sheet3')
sheet3.set_dataframe(table3,(1,1))
print("Done")



end_time = datetime.now()
elapsed_time = end_time - start_time

date_time = end_time.strftime("%m/%d/%y %H:%M:%S")

datetime_message = 'Updated at: ' + date_time + '\n'
runtime_message = 'Runtime: ' + str(elapsed_time) + '\n' + '*'*50 +'\n'

with open('update_log.txt','a') as file:
    file.write(datetime_message+runtime_message)


