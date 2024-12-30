import boto3
import csv
import io
import json

s3Client = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(f"Processing file: {key} from bucket: {bucket}")
    
    # Retrieve the object from S3
    response = s3Client.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    
    # Determine the new file name based on the trigger file
    if key == "productdata.json":
        new_key = "processed_product_data.csv"
        
        # Process JSON data (assuming JSON data is a list of records)
        json_data = json.loads(data)
        
        # Convert JSON to CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header (keys of the first item as column names)
        if len(json_data) > 0:
            header = json_data[0].keys()
            writer.writerow(header)
        
        # Write data rows
        for item in json_data:
            writer.writerow(item.values())
        
        output.seek(0)  # Reset pointer to start
        content = output.getvalue()
    
    elif key == "purchase_data.csv":
        new_key = "processed_purchase_data.csv"
        
        # Read CSV content
        csv_data = io.StringIO(data)
        reader = csv.reader(csv_data)
        
        # Write processed CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        
        for row in reader:
            writer.writerow(row)
        
        output.seek(0)  # Reset pointer to start
        content = output.getvalue()
    
    elif key == "customer_data.csv":
        new_key = "processed_customer_data.csv"
        
        # Read CSV content
        csv_data = io.StringIO(data)
        reader = csv.reader(csv_data)
        
        # Write processed CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        
        for row in reader:
            writer.writerow(row)
        
        output.seek(0)  # Reset pointer to start
        content = output.getvalue()
    
    else:
        return {
            'statusCode': 400,
            'body': "Unsupported file type"
        }
    
    # Upload processed file to the new key (S3 bucket)
    s3Client.put_object(Bucket="eshopdatadpdb", Key=new_key, Body=content)
    
    print(f"Processed file uploaded to {bucket}/{new_key}")
    
    return {
        'statusCode': 200,
        'body': f"File {key} processed and uploaded as {new_key}"
    }
