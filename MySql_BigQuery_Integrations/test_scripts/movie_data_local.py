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
              SELECT 
                year, 
                title, 
                genre, 
                avg_vote,
                case
                    when avg_vote <= 3 then  'Poor'
                    when avg_vote > 3 and avg_vote < 7 then  'Average'
                    when avg_vote >= 7 then  'Excellent'
                end as movie_category,
                duration
              FROM imdb_movies;
              """
        if conn:
            df = pd.read_sql(qry, conn)
            conn.close()
            return df
    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def transformations(d):
    """
    Generates derived column of str data based on input column 'd', which is the duration column in the core dataset
    :param d: num val from duration column
    :return:
      - Str val added to each row in derived column based on num val
    """
    if d < 60:
        return 'Short Film'
    elif d < 120:
        return 'Avg. Length Film'
    elif d < 120:
        return 'Long Film'
    else:
        return 'No Data'


def load():
    """
    Generates .csv files based on distinct vals in 'Year' column of df
    :return:
      -n number of files with movie data segregated by year
    """
    try:
        df = extract()
        if df.empty:
            raise ValueError("DataFrame is empty. Cannot export to CSV.")

        # Apply transformation
        df['length_category'] = df['duration'].apply(transformations)

        cur_path = os.getcwd()
        output_dir = os.path.join(cur_path, '../movie_data')
        os.makedirs(output_dir, exist_ok=True)  # Create the output directory if it doesn't exist

        # Iterate over unique 'year' values
        for year_value in df['year'].unique():
            # Filter DataFrame based on 'year' value
            year_df = df[df['year'] == year_value]
            if not year_df.empty:
                file_name = f"movies_{year_value}.csv"
                file_path = os.path.join(output_dir, file_name)

                # Export subset of DataFrame to CSV
                year_df.to_csv(file_path, index=False)
                print(f"Subset of DataFrame for year {year_value} successfully exported to: {file_path}")
            else:
                print(f"No data for year {year_value}. Skipping export.")

    except FileNotFoundError:
        print("Error: Specified file path does not exist.")
    except PermissionError:
        print("Error: Permission denied while writing to file.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == '__main__':
    load()
