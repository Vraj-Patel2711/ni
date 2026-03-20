import logging
import json
import os
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('NutritionalInsights function triggered.')

    try:
        # Get connection string from environment variable
        conn_str = os.environ.get('STORAGE_CONNECTION_STRING')

        # Connect to Azure Blob Storage
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        blob_client = blob_service.get_blob_client(container='diets-data', blob='All_Diets.csv')

        # Download and read the CSV
        data = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(data))

        # Clean missing values
        df.fillna(df.mean(numeric_only=True), inplace=True)

        # Calculate average macronutrients per diet type
        avg_macros = df.groupby('Diet_type')[['Protein(g)', 'Carbs(g)', 'Fat(g)']].mean().round(2)

        # Build result
        result = {
            'avg_macros': avg_macros.reset_index().to_dict(orient='records'),
            'total_recipes': len(df)
        }

        return func.HttpResponse(
            json.dumps(result),
            mimetype='application/json',
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({'error': str(e)}),
            mimetype='application/json',
            status_code=500
        )