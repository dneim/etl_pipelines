# MySQL & BigQuery Integration - ETL Project

This project is an example of an Extract, Transform, Load (ETL) process for processing data from a source database (MySql), transforming it, and loading it into a destination database (BigQuery).

## Table of Contents

- [Introduction](#introduction)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This projects adapts data from the Udemy course ETL using Python: from MySQL to BigQuery [https://www.udemy.com/course/etl-using-python-mysql-to-bigquery], creating a functional approach 
to the integration of MySql and Google BigQuery. Movie data and house pricing data is adapted from the sample databases provided in the course. For both datasets a local and non-local (to BigQuery) 
load process is presented. 

- Housing data: configured to replace tbl in BigQUery with each run. Code is set to delete prior data and update with data from the latest run to simulate the upkeep of a reference table.
- Movie data: configured to update tbl in BigQUery with each run. Code is set to append data to relevant files with each successive run to simulate appending to a master table.

## Technologies Used

- Python
- [Google Cloud BigQuery](https://cloud.google.com/bigquery) - Google's fully managed, serverless data warehouse for large-scale analytics.
- [Google Cloud Client Library for Python](https://googleapis.dev/python/bigquery/latest/index.html) - Provides client libraries and tools for interacting with BigQuery.
- [MySQL Connector for Python](https://dev.mysql.com/doc/connector-python/en/) - MySQL driver for Python, enabling access to MySQL databases.
- [Pandas](https://pandas.pydata.org/) - Python library for data manipulation and analysis.
- [Google Cloud Client Library for Python (Auth)](https://google-auth.readthedocs.io/en/latest/index.html) - Provides authentication and authorization for accessing Google Cloud services.
- [OS Module](https://docs.python.org/3/library/os.html) - Python module providing a portable way to use operating system-dependent functionality.

## Project Structure

MySql_BigQuery_Integrations/

│

├── final_scripts/

│ ├── house_price_data.py

│ └── movie_data.py

│

├── house_price_data/

│ ├── house_prices.csv

│

├── movie_data/

│ ├── movies_1906.csv

│ ├── movies_1911.csv

│ ├── ...

│
├── test_scripts/

│ ├── code_sandbox.py

│ ├── house_price_data_gbq.py

│ └── house_price_data_local.py

│ └── movie_data_gbq.py

│ └── movie_data_local.py

│

└── Pipfile

└── Pipfile.lock

└── README.md

- **final_scripts/**: Directory containing finalized scripts for each dataset w/ local and BigQuery load funcs.
- **house_price_data/**: Directory containing .csv generated by load_local() func in house_price_data.py.
- **movie_data/**: Directory containing .csv files generated from load_local() func in movie_data.py.
- **test_scripts/**: Directory containing draft files used to construct each ETL job.
- **Pipfile/**: Pipfile for project.
- **Pipfile.lock/**: .lock file for virtual env in local.
- **README.md**: Documentation file providing information about the project.

## Setup Instructions

1. Clone the repository:
   git clone https://github.com/yourusername/ETL_Project.git
2. Install Python dependencies:
   pip install google-cloud-bigquery
   pip install mysql-connector-python
   pip install pandas
   pip install google-auth

3. Ensure you have MySQL WorkBench installed or set up a compatible database.

## Usage

### For local load:
1. Initialize directory for .csv files in same parent dir as where you put either finalized .py file.
2. Comment in load_local() only.

### For BigQuery load:
1. For house_price_data.py:
   a. Program will read in df from transfromations() -- no additional actions needed.
2. For movie_data.py:
   a. Make sure movie_data.py is in the same dir as sub-folder containing .csv move data.
   b. Specify name of directory holding movie data in __main__

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
