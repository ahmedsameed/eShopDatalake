import pyodbc 
import pandas as pd
import boto3
import json

# Connection string for Windows Authentication
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=AHMED;"  # Replace with your server and instance name
    "DATABASE=carrentalsystem;"
    "Trusted_Connection=yes;"
)

CSV_FILE_PATH = "C:/Users/ahmed/Desktop/DPDB project/raw source data purchase and click stream/purchase_history.csv"
JSON_FILE_PATH = "C:/Users/ahmed/Desktop/DPDB project/raw source data purchase and click stream/productdata.json"

S3_BUCKET_NAME = "eshopdataraw"
S3_FOLDERPRO = "processedeshop"
S3_FOLDERRAW = "eshopdataraw"


s3_client = boto3.client('s3')

def extract_from_csv(file_path):
    return pd.read_csv(file_path)

def extract_from_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return pd.json_normalize(data)

def extract_from_sqlserver(query):
    try:
        connection = pyodbc.connect(CONNECTION_STRING)
        data = pd.read_sql(query, connection)
        connection.close()
        return data
    except pyodbc.Error as e:
        print("Error while connecting to SQL Server:", e)

def transform_data(dataframe, transformations):
    for transformation in transformations:
        dataframe = transformation(dataframe)
    return dataframe

def cleanse_data(df):
    return df.dropna() 

def convert_types(df, column_types):
    for column, dtype in column_types.items():
        df[column] = df[column].astype(dtype)
    return df

def format_timestamps(df, timestamp_column):
    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    return df

def load_to_s3(dataframe, s3_bucket, s3_key):
    print(s3_key)
    csv_buffer = dataframe.to_csv(index=False)
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer)

def upload_json_to_s3(local_file_path, s3_bucket, s3_key):
    # Open the local JSON file and read its content
    with open(local_file_path, 'r') as file:
        json_data = file.read()  # Read the entire file content
    
    # Upload the content to S3 as a JSON file
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=json_data, ContentType='application/json') 


customer_query = "SELECT * FROM CustomerTable;"
customer_data = extract_from_sqlserver(customer_query)
load_to_s3(customer_data, S3_BUCKET_NAME, f"customer_data.csv")

purchase_data=extract_from_csv(CSV_FILE_PATH)
load_to_s3(purchase_data, S3_BUCKET_NAME, f"purchase_data.csv")


upload_json_to_s3(JSON_FILE_PATH, S3_BUCKET_NAME, f"productdata.json")

#print(type(customer_data))
#print(customer_data)
#print(customer_data.lastname)

#transaction_data = extract_from_csv(CSV_FILE_PATH)
#load_to_s3(transaction_data, S3_BUCKET_NAME, f"{S3_FOLDERRAW}/transaction_data.csv")
#print(transaction_data)

'''product_data=extract_from_json(JSON_FILE_PATH)
load_to_s3(product_data, S3_BUCKET_NAME, f"product_data2.csv")
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns

# Print the entire DataFrame
#print(product_data)



customer_data=transform_data(customer_data, [cleanse_data, lambda df: convert_types(df, {'personID': int})])

transaction_data = transform_data(transaction_data, [cleanse_data, lambda df: format_timestamps(df, 'SignupDate')])
#print(transaction_data)
load_to_s3(customer_data, S3_BUCKET_NAME, f"{S3_FOLDERPRO}/customer_data.csv")
#print("ENGINE START")
#print(product_data)
load_to_s3(transaction_data, S3_BUCKET_NAME, f"{S3_FOLDERPRO}/transaction_data.csv")
load_to_s3(product_data, S3_BUCKET_NAME, f"{S3_FOLDERPRO}/product_data.csv")'''
