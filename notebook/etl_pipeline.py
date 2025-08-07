# Import necessary Libraries
import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv


#logging
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(BASE_DIR, '..', 'dataset', 'ziko_logistics_data.csv')




#Extract data from CSV files
ziko_df = pd.read_csv(r'../dataset/ziko_logistics_data.csv')

#Data Cleaning and Transformation
ziko_df.fillna({
    'Unit_Price': ziko_df['Unit_Price'].mean(),
    'Total_Cost': ziko_df['Total_Cost'].mean(),
    'Discount_Rate': 0.0,
    'Return_Reason': 'Unknown'

}, inplace=True) 

ziko_df['Date']= pd.to_datetime(ziko_df['Date'], errors='coerce')

#Customer Table
customer =ziko_df[['Customer_ID', 'Customer_Name', 'Customer_Email', 'Customer_Phone', 'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)
customer.head()


#product Table
product = ziko_df[['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price', 'Total_Cost','Discount_Rate']].copy().drop_duplicates().reset_index(drop=True)
product.head()


#Transaction Fact Table
transaction_fact = ziko_df.merge(customer, on =['Customer_ID', 'Customer_Name', 'Customer_Email', 'Customer_Phone', 'Customer_Address'], how ='left' ) \
                          .merge(product, on=['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price', 'Total_Cost',], how ='left') \
                          [['Transaction_ID', 'Date', 'Customer_ID', 'Product_ID','Sales_Channel','Order_Priority', \
                            'Warehouse_Code', 'Ship_Mode', 'Delivery_Status', 'Customer_Satisfaction', 'Item_Returned', 'Return_Reason','Payment_Type', 'Taxable', 'Region', 'Country']]


#Temporary loading
customer.to_csv(r'../dataset/customer.csv', index=False)
product.to_csv(r'../dataset/product.csv', index=False)
transaction_fact.to_csv(r'../dataset/transaction_fact.csv', index=False)

print('files have been loaded temporary into local machine')


# Data Loading
# Azure blob connection

load_dotenv()

connect_str = os.getenv('CONNECT_STR')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)


#create a function to load dataframes to Azure Blob Storage as parquet files
def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine='fastparquet')
        buffer.seek(0)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(buffer, overwrite=True)
        print(f"uploaded to Azure Blob Storage successfully! {blob_name}")
    except Exception as e:
        print(f"Error uploading {blob_name}: {e}")


upload_df_to_blob_as_parquet(customer, container_client, 'rawdata/customer.parquet')
upload_df_to_blob_as_parquet(product, container_client, 'rawdata/product.parquet')
upload_df_to_blob_as_parquet(transaction_fact, container_client, 'rawdata/transaction_fact.parquet')


