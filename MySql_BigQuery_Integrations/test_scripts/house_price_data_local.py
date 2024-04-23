# modules
import mysql.connector
import pandas as pd
import os

# set df display options for testing:
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)


def mysql_connector():
    """
    Connect ot MySql using credentials stored in a .txt file.
    :return:
      conn: connection object
    """
    try:
        # Construct the path to the credentials file
        cur_path = os.getcwd()
        creds_path = os.path.join(cur_path, "../creds.txt")

        with open(creds_path, "r") as file:
            credentials = {}
            for line in file:
                if "=" in line:  # Check for lines containing '='
                    key, value = line.strip().split("=")
                    credentials[key.strip()] = value.strip()

        # Establish connection
        conn = mysql.connector.connect(**credentials)

        print("Connection Successful!")
        return conn
    except FileNotFoundError:
        print("Credentials file not found!")
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")


def extract():
    """
    Execute MySql query, specified in 'qry' object.
    :return:
      df: DataFrame object of MySQL query results
    """
    try:
        conn = mysql_connector()
        qry = """
                SELECT * 
                FROM city_house_prices;
              """
        if conn:
            df = pd.read_sql(qry, conn)
            conn.close()
            return df
    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def transformations():
    """
    Ingests df form extract() and performs following transformations:
        - Pivots core data so that all prices for a given state are ordered by Date
        - Split State-City values into distinct columns
    :return:
      - df: transformed DataFrame object
    """
    try:
        df = extract()
        if df.empty:
            raise ValueError("DataFrame is empty. Cannot perform transformations.")

        df.set_index('Date', inplace=True)
        df = df.stack().reset_index()
        df.columns = ['Date', 'City', 'Price']

        # Splitting the City column into State and City columns
        df[['State', 'City']] = df['City'].str.split('-', expand=True)

        # Rearrange the columns
        df = df[['Date', 'City', 'State', 'Price']]

        return df

    except KeyError:
        print("Error: 'Date' column does not exist in the DataFrame.")
    except ValueError as ve:
        print(ve)
    except Exception as e:
        print("An unexpected error occurred:", e)


def load():
    """
    Loads df data into specified local dir as .csv
    :return: None
    """
    try:
        df = transformations()
        if df.empty:
            raise ValueError("DataFrame is empty. Cannot export to CSV.")

        cur_path = os.getcwd()
        output_dir = os.path.join(cur_path, '../house_price_data')
        os.makedirs(output_dir, exist_ok=True)  # Create the output directory if it doesn't exist

        file_name = "house_prices.csv"
        file_path = os.path.join(output_dir, file_name)

        # Export subset of DataFrame to CSV
        df.to_csv(file_path, index=False)

    except FileNotFoundError:
        print("Error: Specified file path does not exist.")
    except PermissionError:
        print("Error: Permission denied while writing to file.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == '__main__':
    load()
