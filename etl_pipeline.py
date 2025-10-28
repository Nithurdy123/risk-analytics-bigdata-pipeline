"""
ETL Pipeline - Beginner Project
--------------------------------
This script demonstrates a simple Extract, Transform, Load (ETL) process
for a basic financial risk analytics pipeline.

Author: Nithya Gaddam
"""

import pandas as pd

def extract_data():
    data = {
        "CustomerID": [1, 2, 3, 4],
        "LoanAmount": [10000, 25000, 15000, 30000],
        "RiskScore": [0.2, 0.8, 0.5, 0.9]
    }
    df = pd.DataFrame(data)
    print("✅ Data Extracted:")
    print(df)
    return df

def transform_data(df):
    df["RiskLevel"] = df["RiskScore"].apply(lambda x: "High" if x >= 0.7 else "Low")
    print("\n✅ Data Transformed with Risk Level:")
    print(df)
    return df

def load_data(df):
    df.to_csv("risk_data_output.csv", index=False)
    print("\n✅ Data Loaded to 'risk_data_output.csv'")

if __name__ == "__main__":
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)
    print("\n🚀 ETL Pipeline Completed Successfully!")
