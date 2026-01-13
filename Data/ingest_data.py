import pandas as pd 
import os
import io
import requests
import zipfile
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv


#Loading DataSet
load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
db = os.getenv("POSTGRES_DB")
host = "localhost"
port = "5432"

if not user or not password:
    raise ValueError("Error: Could not find Credentials")


# Database Connection 

db_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
engine = create_engine(db_str)

# Data Sources
# E-commerce SAP Sales
olist_url = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_items_dataset.csv"
olist_orders_url = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_orders_dataset.csv"
olist_product_url = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_products_dataset.csv"
# IOT Data Azure
iot_url = "https://archive.ics.uci.edu/static/public/601/ai4i+2020+predictive+maintenance+dataset.zip"


def ingest_data():
    print(f"Connecting to database '{db} as user '{user}'")

    #Inserting Sales Data 
    print("Downloading E commerce Data")
    df_items = pd.read_csv(olist_url)
    df_headers = pd.read_csv(olist_orders_url)
    df_products =pd.read_csv(olist_product_url)

    print("Mergeing Data and Formating")
    df_sales = pd.merge(df_headers, df_items, on ="order_id")
    df_sales = pd.merge(df_sales, df_products, on="product_id")

    df_sales = df_sales.rename(columns={
            "order_id": "VBELN", 
            "order_purchase_timestamp": "AUDAT",
            "product_id": "MATNR", 
            "price": "NETWR",
            "product_category_name": "CATEGORY" 
        })

    print(f"Loading{len(df_sales)} sales rows into progres")
    cols = ["VBELN", "AUDAT", "MATNR", "NETWR", "CATEGORY"]
    df_sales.to_sql('sap_sales_flat', engine, if_exists='replace', index=False)
    print(f"Loaded {len(df_sales)} Sales Order")

    unique_categories = df_sales['CATEGORY'].unique()


    # Ingesting IOT data
    print("Downloading IOT Data")
    try:
        r = requests.get(iot_url)
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            csv_name = [f for f in z.namelist() if f.endswith('.csv')][0]
            with z.open(csv_name) as f:
                df_iot = pd.read_csv(f)

        df_iot = df_iot.rename(columns={
            "Type": "Machine_Type",
            "Air temperature [K]": "Air_Temp",
            "Process temperature [K]": "Process_Temp",
            "Rotational speed [rpm]": "Rotational_Speed",
            "Torque [Nm]": "Torque",
            "Tool wear [min]": "Tool_Wear",
            "Machine failure": "Failure_Label"
        })

        end_date = pd.Timestamp.now()
        periods = len(df_iot)
        df_iot['Timestamp'] = pd.date_range(end=end_date, periods=periods, freq='5min')

        df_iot.to_sql('iot_sensors', engine, if_exists='replace', index=False)
        print(f"Loaded {len(df_iot)} Machine Sensor Readings.")

    except Exception as e:
        print(f"Error in IoT Data: {e}")
        return

if __name__ == "__main__":
    ingest_data()

