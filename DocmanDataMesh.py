import csv
import pyodbc
import requests
import re

# Define the CSV file and Azure SQL Database connection details
csv_file = './Inventory.csv'
server = 'labelcloud-sql-production-deploc5-user.database.windows.net'
database = 'docman'
username = 'admin'
password = 'Docman927'
table_name1 = 'InventoryTable'
table_name2 = 'sample_data'

# Define the API endpoint & Token
api_endpoint = 'https://qwc2nuifoe.execute-api.us-east-1.amazonaws.com/v1/ar/label/'
api_token = 'q3o4rutgsdiofbuj!sh'

# Create a connection to the Azure SQL Database
conn = pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')

# Create a cursor to execute SQL commands
cursor = conn.cursor()

# #### MAIN ###################################################################################################################### 
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
    
    # This function performs the API call and returns the response
    def make_api_call(inventory_id):
        headers = {
            'Authorization': f'{api_token}'
        }
        appended_api_endpoint = f'{api_endpoint}{inventory_id}'
        response = requests.get(appended_api_endpoint, headers=headers)
        print(f"API ENDPOINT: {appended_api_endpoint}")
        if response.status_code == 200:
            
            response = response.json()
            #print(f"Original RESPONSE: {response}")
            
            response = sanitize_api_response(response)
            #print(f"Sanitized RESPONSE: {response}")
                        
            # Insert JSON response body into test_results table
            insert_sample_data(response)
            
            print(f"API Success")
            
        else:
            print(f"API Failed Status code: {response.status_code}")        

        return response
    
    # Replace NONE with 'NULL' in API response
    def sanitize_api_response(response):
        sanitized_response = {}

        for key, value in response.items():
            if value is None:
                sanitized_response[key] = 'NULL'
            else:
                sanitized_response[key] = value
        return sanitized_response

    # This function takes the response from the api for one Invetory ID and inserts it into the db
    def insert_sample_data(response):
        insert_sample_data_sql = f"INSERT INTO {table_name2} " \
                f"(sample_id, bt_sample_id, order_date_received, sample_name, s_matrix, order_id, package_mass, servings, " \
                f"final_cbc_mg_g, final_cbc_mg_g_dry, final_cbc_perc, final_cbc_perc_dry, final_cbc_mg_pkg, final_cbc_mg_serving, " \
                f"final_cbd_mg_g, final_cbd_mg_g_dry, final_cbd_perc, final_cbd_perc_dry, final_cbd_mg_pkg, final_cbd_mg_serving, " \
                f"final_cbda_mg_g, final_cbda_mg_g_dry, final_cbda_perc, final_cbda_perc_dry, final_cbda_mg_pkg, final_cbda_mg_serving, " \
                f"final_cbdv_mg_g, final_cbdv_mg_g_dry, final_cbdv_perc, final_cbdv_perc_dry, final_cbdv_mg_pkg, final_cbdv_mg_serving, " \
                f"final_cbg_mg_g, final_cbg_mg_g_dry, final_cbg_perc, final_cbg_perc_dry, final_cbg_mg_pkg, final_cbg_mg_serving, " \
                f"final_cbga_mg_g, final_cbga_mg_g_dry, final_cbga_perc, final_cbga_perc_dry, final_cbga_mg_pkg, final_cbga_mg_serving, " \
                f"final_cbn_mg_g, final_cbn_mg_g_dry, final_cbn_perc, final_cbn_perc_dry, final_cbn_mg_pkg, final_cbn_mg_serving, " \
                f"final_d8_thc_mg_g, final_d8_thc_mg_g_dry, final_d8_thc_perc, final_d8_thc_perc_dry, final_d8_thc_mg_pkg, final_d8_thc_mg_serving, " \
                f"final_thc_mg_g, final_thc_mg_g_dry, final_thc_perc, final_thc_perc_dry, final_thc_mg_pkg, final_thc_mg_serving, " \
                f"final_thca_mg_g, final_thca_mg_g_dry, final_thca_perc, final_thca_perc_dry, final_thca_mg_pkg, final_thca_mg_serving, " \
                f"final_thcv_mg_g, final_thcv_mg_g_dry, final_thcv_perc, final_thcv_perc_dry, final_thcv_mg_pkg, final_thcv_mg_serving, " \
                f"final_total_canna_mg_g, final_total_canna_mg_g_dry, final_total_canna_perc, final_total_canna_perc_dry, final_total_canna_mg_pkg, final_total_canna_mg_serving, " \
                f"final_total_cbd_mg_g, final_total_cbd_mg_g_dry, final_total_cbd_perc, final_total_cbd_perc_dry, final_total_cbd_mg_pkg, final_total_cbd_mg_serving, " \
                f"final_total_thc_mg_g, final_total_thc_mg_g_dry, final_total_thc_perc, final_total_thc_perc_dry, final_total_thc_mg_pkg, final_total_thc_mg_serving, " \
                f"final_a_pinene_perc, final_b_pinene_perc, final_b_myrcene_perc, final_limonene_perc, final_terpinolene_perc, final_linalool_perc, " \
                f"final_b_caryophyllene_perc, final_a_humulene_perc, final_caryophyllene_oxide_perc, final_a_bisabolol_perc, final_camphene_perc, " \
                f"final_3_carene_perc, final_cymene_perc, final_eucalyptol_perc, final_geraniol_perc, final_guaiol_perc, final_trans_nerolidol_perc, " \
                f"final_cis_b_ocimene_perc, final_a_terpinene_perc, final_y_terpinene_perc, final_total_terp_perc) " \
                f"VALUES " \
                f"('{response['sample_id']}', '{response['bt_sample_id']}', '{response['order_date_received']}', " \
                f"'{response['sample_name']}', '{response['s_matrix']}', {response['order_id']}, " \
                f"{response['package_mass']}, {response['servings']}, " \
                f"{response['final_cbc_mg_g']}, {response['final_cbc_mg_g_dry']}, {response['final_cbc_perc']}, {response['final_cbc_perc_dry']}, {response['final_cbc_mg_pkg']}, {response['final_cbc_mg_serving']}, " \
                f"{response['final_cbd_mg_g']}, {response['final_cbd_mg_g_dry']}, {response['final_cbd_perc']}, {response['final_cbd_perc_dry']}, {response['final_cbd_mg_pkg']}, {response['final_cbd_mg_serving']}, " \
                f"{response['final_cbda_mg_g']}, {response['final_cbda_mg_g_dry']}, {response['final_cbda_perc']}, {response['final_cbda_perc_dry']}, {response['final_cbda_mg_pkg']}, {response['final_cbda_mg_serving']}, " \
                f"{response['final_cbdv_mg_g']}, {response['final_cbdv_mg_g_dry']}, {response['final_cbdv_perc']}, {response['final_cbdv_perc_dry']}, {response['final_cbdv_mg_pkg']}, {response['final_cbdv_mg_serving']}, " \
                f"{response['final_cbg_mg_g']}, {response['final_cbg_mg_g_dry']}, {response['final_cbg_perc']}, {response['final_cbg_perc_dry']}, {response['final_cbg_mg_pkg']}, {response['final_cbg_mg_serving']}, " \
                f"{response['final_cbga_mg_g']}, {response['final_cbga_mg_g_dry']}, {response['final_cbga_perc']}, {response['final_cbga_perc_dry']}, {response['final_cbga_mg_pkg']}, {response['final_cbga_mg_serving']}, " \
                f"{response['final_cbn_mg_g']}, {response['final_cbn_mg_g_dry']}, {response['final_cbn_perc']}, {response['final_cbn_perc_dry']}, {response['final_cbn_mg_pkg']}, {response['final_cbn_mg_serving']}, " \
                f"{response['final_d8_thc_mg_g']}, {response['final_d8_thc_mg_g_dry']}, {response['final_d8_thc_perc']}, {response['final_d8_thc_perc_dry']}, {response['final_d8_thc_mg_pkg']}, {response['final_d8_thc_mg_serving']}, " \
                f"{response['final_thc_mg_g']}, {response['final_thc_mg_g_dry']}, {response['final_thc_perc']}, {response['final_thc_perc_dry']}, {response['final_thc_mg_pkg']}, {response['final_thc_mg_serving']}, " \
                f"{response['final_thca_mg_g']}, {response['final_thca_mg_g_dry']}, {response['final_thca_perc']}, {response['final_thca_perc_dry']}, {response['final_thca_mg_pkg']}, {response['final_thca_mg_serving']}, " \
                f"{response['final_thcv_mg_g']}, {response['final_thcv_mg_g_dry']}, {response['final_thcv_perc']}, {response['final_thcv_perc_dry']}, {response['final_thcv_mg_pkg']}, {response['final_thcv_mg_serving']}, " \
                f"{response['final_total_canna_mg_g']}, {response['final_total_canna_mg_g_dry']}, {response['final_total_canna_perc']}, {response['final_total_canna_perc_dry']}, {response['final_total_canna_mg_pkg']}, {response['final_total_canna_mg_serving']}, " \
                f"{response['final_total_cbd_mg_g']}, {response['final_total_cbd_mg_g_dry']}, {response['final_total_cbd_perc']}, {response['final_total_cbd_perc_dry']}, {response['final_total_cbd_mg_pkg']}, {response['final_total_cbd_mg_serving']}, " \
                f"{response['final_total_thc_mg_g']}, {response['final_total_thc_mg_g_dry']}, {response['final_total_thc_perc']}, {response['final_total_thc_perc_dry']}, {response['final_total_thc_mg_pkg']}, {response['final_total_thc_mg_serving']}, " \
                f"{response['final_a_pinene_perc']}, {response['final_b_pinene_perc']}, {response['final_b_myrcene_perc']}, " \
                f"{response['final_limonene_perc']}, {response['final_terpinolene_perc']}, {response['final_linalool_perc']}, " \
                f"{response['final_b_caryophyllene_perc']}, {response['final_a_humulene_perc']}, {response['final_caryophyllene_oxide_perc']}, " \
                f"{response['final_a_bisabolol_perc']}, {response['final_camphene_perc']}, {response['final_3_carene_perc']}, " \
                f"{response['final_cymene_perc']}, {response['final_eucalyptol_perc']}, {response['final_geraniol_perc']}, " \
                f"{response['final_guaiol_perc']}, {response['final_trans_nerolidol_perc']}, {response['final_cis_b_ocimene_perc']}, " \
                f"{response['final_a_terpinene_perc']}, {response['final_y_terpinene_perc']}, {response['final_total_terp_perc']});"
            
        #print(f"SQL STATEMENT: {insert_sample_data_sql}")
        cursor.execute(insert_sample_data_sql)

    for row in csv_reader:

        try:
            # Remove spaces from Inventory_ID column entries
            row['Inventory ID'] = row['Inventory ID'].replace(" ", "")

            # Call Test Results API passing in the current Inventory ID
            record = make_api_call(row['Inventory ID'])

            # When the CSV was read, all values were stored as strings. We must make sure they are in the correct datatype befor inserting into db        
            sanitized_values = []
            

            for value in row.values():
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
                        escaped_single_quote = value.replace("'", "''")
                        sanitized_values.append(f"'{escaped_single_quote}'")

            # Construct the SQL INSERT statement
            columns = ', '.join(sanitize_column_names(column_names))
            values = ', '.join(sanitized_values)
            insert_sql = f"INSERT INTO {table_name1} ({columns}) VALUES ({values});"

            # Print the SQL statement for review
            #print("Executing SQL statement:", insert_sql)

            # Execute the INSERT statement
            cursor.execute(insert_sql)
        
        except pyodbc.Error as e:
            print(f"Error in row: {row}")
            print(f"Error message: {str(e)}")
            #print(f"There was an error with Inventory insert.")

# Commit the changes and close the database connection
conn.commit()
conn.close()
