import csv
import pyodbc
import re

# Define your CSV file and Azure SQL Database connection details
csv_file = 'D:\Microsoft SQL Studio Stuff\Inventory.csv'
server = 'labelcloud-sql-production-deploc5-user.database.windows.net'
database = 'docman'
username = 'admin'
password = 'Docman927'
table_name1 = 'InventoryTable'
table_name2 = 'TestResults'

# Create a connection to the Azure SQL Database
conn = pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')

# Create a cursor to execute SQL commands
cursor = conn.cursor()

# Open the CSV file for reading
### encoding=utf-8-sig needed to avoid including the UTF-8 byte order mark
with open(csv_file, 'r', encoding='utf-8-sig') as csv_file:
    csv_reader = csv.DictReader(csv_file)

    column_names = csv_reader.fieldnames

    # This function prepares the column names for the sql insert statement
    # SQL needs column names with spaces or special characters to be enclosed in []
    def sanitize_column_names(column_names):
        sanitized_names = []
        for name in column_names:
            if re.search(r'[^\w]', name):  # Check for any non-word characters
                sanitized_names.append(f'[{name}]')
            else:
                sanitized_names.append(name)
        return sanitized_names
    
    for row in csv_reader:

        try:
            # Remove spaces from Inventory_ID column entries
            row['Inventory ID'] = row['Inventory ID'].replace(" ", "")

            # TODO: Call test results API appending the value in the Inventory ID column to the api endpoint
            # TODO: Parse the JSON response and INSERT into a db table

            # When the CSV was read, all values were stored as strings. We must make sure they are in the correct datatype befor inserting into db        
            sanitized_values = []

            for value in row.values():
                 # Remove single quotes from numeric values 
                value.strip("'")

                if value.isdigit():
                    sanitized_values.append(value)
                elif not value:
                    sanitized_values.append('NULL')
                else:
                    try:
                        float_value = float(value)
                        sanitized_values.append(float_value)
                    except ValueError:
                        sanitized_values.append(f"'{value}'")

            # Construct the SQL INSERT statement
            columns = ', '.join(sanitize_column_names(column_names))
            values = ', '.join(sanitized_values)
            insert_sql = f"INSERT INTO {table_name1} ({columns}) VALUES ({values});"

            # Print the SQL statement for review
            print("Executing SQL statement:", insert_sql)

            # Execute the INSERT statement
            cursor.execute(insert_sql)
        
        except pyodbc.Error as e:
            print(f"Error in row: {row}")
            print(f"Error message: {str(e)}")
            for column, value in row.items():
                try:
                    # Attempt to convert the value to the expected data type
                    int(value)  # You can use a different data type conversion function if needed
                except ValueError:
                    print(f"Error in column: {column}")

# Commit the changes and close the database connection
conn.commit()
conn.close()
