import logging
import json
import pandas as pd
import io
import os
import time
import hashlib
import secrets
from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient
import azure.functions as func

app = func.FunctionApp()

# ========== TABLE STORAGE CACHING ==========
def get_cache_table():
    conn_str = os.environ.get("AzureWebJobsStorage")
    table_service = TableServiceClient.from_connection_string(conn_str)
    return table_service.get_table_client("CacheResults")

# Create cache table if it doesn't exist
try:
    cache_table = get_cache_table()
    cache_table.create_table()
    logging.info("Cache table created/available")
except Exception as e:
    logging.info(f"Cache table already exists: {str(e)}")

# ========== USER AUTHENTICATION ==========

def get_user_table():
    conn_str = os.environ.get("AzureWebJobsStorage")
    table_service = TableServiceClient.from_connection_string(conn_str)
    return table_service.get_table_client("Users")

try:
    table_client = get_user_table()
    table_client.create_table()
    logging.info("Users table created/available")
except Exception as e:
    logging.info(f"Users table already exists: {str(e)}")

@app.route(route="register", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def Register(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        email = req_body.get("email")
        password = req_body.get("password")
        
        if not email or not password:
            return func.HttpResponse(
                json.dumps({"error": "Email and password required"}),
                mimetype="application/json",
                status_code=400
            )
        
        salt = secrets.token_hex(16)
        hashed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        password_hash = salt + ":" + hashed.hex()
        
        table_client = get_user_table()
        entity = {
            "PartitionKey": "User",
            "RowKey": email,
            "Email": email,
            "PasswordHash": password_hash
        }
        table_client.create_entity(entity)
        
        return func.HttpResponse(
            json.dumps({"message": "User registered successfully", "user": email}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        if "EntityAlreadyExists" in str(e):
            return func.HttpResponse(
                json.dumps({"error": "User already exists"}),
                mimetype="application/json",
                status_code=409
            )
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.route(route="login", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def Login(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        email = req_body.get("email")
        password = req_body.get("password")
        
        if not email or not password:
            return func.HttpResponse(
                json.dumps({"error": "Email and password required"}),
                mimetype="application/json",
                status_code=400
            )
        
        table_client = get_user_table()
        
        try:
            entity = table_client.get_entity("User", email)
            stored_hash = entity["PasswordHash"]
        except Exception:
            return func.HttpResponse(
                json.dumps({"error": "Invalid credentials"}),
                mimetype="application/json",
                status_code=401
            )
        
        salt, stored = stored_hash.split(":")
        computed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()
        
        if computed == stored:
            return func.HttpResponse(
                json.dumps({"message": "Login successful", "user": email}),
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                json.dumps({"error": "Invalid credentials"}),
                mimetype="application/json",
                status_code=401
            )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

# ========== BLOB TRIGGER ==========
@app.blob_trigger(arg_name="myblob", path="diet-data/{name}",
                  connection="AzureWebJobsStorage")
def ProcessDietData(myblob: func.InputStream):
    logging.info(f"File changed: {myblob.name}")
    
    try:
        df = pd.read_csv(myblob)
        logging.info(f"Loaded {len(df)} rows")
        
        if 'Recipe_name' in df.columns:
            df = df.drop_duplicates(subset=['Recipe_name'])
        
        nutrient_cols = ['Protein(g)', 'Carbs(g)', 'Fat(g)']
        for col in nutrient_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        if 'Diet_type' in df.columns:
            df['Diet_type'] = df['Diet_type'].str.lower().str.strip()
        
        logging.info(f"Cleaned data: {len(df)} rows remain")
        
        conn_str = os.environ.get("AzureWebJobsStorage")
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        
        try:
            container_client = blob_service.get_container_client("cleaned-data")
            container_client.get_container_properties()
        except:
            container_client = blob_service.create_container("cleaned-data")
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        container_client.upload_blob("cleaned_diets.csv", 
                                       csv_buffer.getvalue().encode('utf-8'),
                                       overwrite=True)
        logging.info("Saved cleaned data to cleaned-data/cleaned_diets.csv")
        
        # Pre-calculate and cache results
        try:
            cache_table = get_cache_table()
            
            # Calculate average nutrients
            result = {}
            if 'Diet_type' in df.columns:
                for diet in df['Diet_type'].unique():
                    diet_df = df[df['Diet_type'] == diet]
                    result[diet] = {
                        "Protein_g": float(diet_df['Protein(g)'].mean()),
                        "Carbs_g": float(diet_df['Carbs(g)'].mean()),
                        "Fat_g": float(diet_df['Fat(g)'].mean())
                    }
            
            # Calculate distribution
            distribution = {}
            if 'Diet_type' in df.columns:
                distribution = df['Diet_type'].value_counts().to_dict()
            
            # Calculate scatter data
            scatter = []
            if 'Protein(g)' in df.columns and 'Carbs(g)' in df.columns:
                scatter = df[['Protein(g)', 'Carbs(g)', 'Diet_type']].dropna().to_dict('records')
            
            # Store in cache table
            cache_table.upsert_entity({
                "PartitionKey": "Cache",
                "RowKey": "nutrients",
                "Value": json.dumps(result)
            })
            
            cache_table.upsert_entity({
                "PartitionKey": "Cache",
                "RowKey": "distribution",
                "Value": json.dumps(distribution)
            })
            
            cache_table.upsert_entity({
                "PartitionKey": "Cache",
                "RowKey": "scatter",
                "Value": json.dumps(scatter)
            })
            
            logging.info("Cached results in Table Storage successfully")
        except Exception as e:
            logging.error(f"Cache storage failed: {str(e)}")
        
    except Exception as e:
        logging.error(f"Error in ProcessDietData: {str(e)}")

# ========== HTTP FUNCTIONS WITH CACHE ==========

@app.route(route="nutrients", auth_level=func.AuthLevel.ANONYMOUS)
def GetNutrients(req: func.HttpRequest) -> func.HttpResponse:
    start = time.time()
    
    try:
        # Try to get from cache table first
        try:
            cache_table = get_cache_table()
            entity = cache_table.get_entity("Cache", "nutrients")
            elapsed = (time.time() - start) * 1000
            return func.HttpResponse(
                json.dumps({
                    "data": json.loads(entity["Value"]),
                    "from_cache": True,
                    "response_time_ms": round(elapsed)
                }),
                mimetype="application/json"
            )
        except Exception as e:
            logging.info(f"Cache miss: {str(e)}")
        
        # Cache miss - read from blob and calculate
        conn_str = os.environ.get("AzureWebJobsStorage")
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        
        try:
            container_client = blob_service.get_container_client("cleaned-data")
            blob_client = container_client.get_blob_client("cleaned_diets.csv")
            content = blob_client.download_blob().readall()
        except:
            container_client = blob_service.get_container_client("diet-data")
            blob_client = container_client.get_blob_client("All_Diets.csv")
            content = blob_client.download_blob().readall()
        
        df = pd.read_csv(io.BytesIO(content))
        
        result = {}
        if 'Diet_type' in df.columns:
            for diet in df['Diet_type'].unique():
                diet_df = df[df['Diet_type'] == diet]
                result[diet] = {
                    "Protein_g": float(diet_df['Protein(g)'].mean()),
                    "Carbs_g": float(diet_df['Carbs(g)'].mean()),
                    "Fat_g": float(diet_df['Fat(g)'].mean())
                }
        
        # Store in cache for next time
        try:
            cache_table = get_cache_table()
            cache_table.upsert_entity({
                "PartitionKey": "Cache",
                "RowKey": "nutrients",
                "Value": json.dumps(result)
            })
            logging.info("Cached nutrients result")
        except Exception as e:
            logging.warning(f"Failed to cache: {str(e)}")
        
        elapsed = (time.time() - start) * 1000
        
        return func.HttpResponse(
            json.dumps({
                "data": result,
                "from_cache": False,
                "response_time_ms": round(elapsed)
            }),
            mimetype="application/json"
        )
    
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.route(route="scatter", auth_level=func.AuthLevel.ANONYMOUS)
def GetScatter(req: func.HttpRequest) -> func.HttpResponse:
    start = time.time()
    
    try:
        try:
            cache_table = get_cache_table()
            entity = cache_table.get_entity("Cache", "scatter")
            elapsed = (time.time() - start) * 1000
            return func.HttpResponse(
                json.dumps({
                    "data": json.loads(entity["Value"]),
                    "from_cache": True,
                    "response_time_ms": round(elapsed)
                }),
                mimetype="application/json"
            )
        except:
            pass
        
        conn_str = os.environ.get("AzureWebJobsStorage")
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        
        try:
            container_client = blob_service.get_container_client("cleaned-data")
            blob_client = container_client.get_blob_client("cleaned_diets.csv")
            content = blob_client.download_blob().readall()
        except:
            container_client = blob_service.get_container_client("diet-data")
            blob_client = container_client.get_blob_client("All_Diets.csv")
            content = blob_client.download_blob().readall()
        
        df = pd.read_csv(io.BytesIO(content))
        
        scatter_data = []
        if 'Protein(g)' in df.columns and 'Carbs(g)' in df.columns:
            scatter_data = df[['Protein(g)', 'Carbs(g)', 'Diet_type']].dropna().to_dict('records')
        
        elapsed = (time.time() - start) * 1000
        
        return func.HttpResponse(
            json.dumps({
                "data": scatter_data,
                "from_cache": False,
                "response_time_ms": round(elapsed)
            }),
            mimetype="application/json"
        )
    
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.route(route="distribution", auth_level=func.AuthLevel.ANONYMOUS)
def GetDistribution(req: func.HttpRequest) -> func.HttpResponse:
    start = time.time()
    
    try:
        try:
            cache_table = get_cache_table()
            entity = cache_table.get_entity("Cache", "distribution")
            elapsed = (time.time() - start) * 1000
            return func.HttpResponse(
                json.dumps({
                    "data": json.loads(entity["Value"]),
                    "from_cache": True,
                    "response_time_ms": round(elapsed)
                }),
                mimetype="application/json"
            )
        except:
            pass
        
        conn_str = os.environ.get("AzureWebJobsStorage")
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        
        try:
            container_client = blob_service.get_container_client("cleaned-data")
            blob_client = container_client.get_blob_client("cleaned_diets.csv")
            content = blob_client.download_blob().readall()
        except:
            container_client = blob_service.get_container_client("diet-data")
            blob_client = container_client.get_blob_client("All_Diets.csv")
            content = blob_client.download_blob().readall()
        
        df = pd.read_csv(io.BytesIO(content))
        
        if 'Diet_type' in df.columns:
            diet_counts = df['Diet_type'].value_counts().to_dict()
        else:
            diet_counts = {}
        
        elapsed = (time.time() - start) * 1000
        
        return func.HttpResponse(
            json.dumps({
                "data": diet_counts,
                "from_cache": False,
                "response_time_ms": round(elapsed)
            }),
            mimetype="application/json"
        )
    
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def ServeDashboard(req: func.HttpRequest) -> func.HttpResponse:
    try:
        with open('static/index.html', 'r') as f:
            content = f.read()
        return func.HttpResponse(content, mimetype='text/html')
    except:
        return func.HttpResponse("Dashboard not found", status_code=404)

@app.route(route="recipes", auth_level=func.AuthLevel.ANONYMOUS)
def GetRecipes(req: func.HttpRequest) -> func.HttpResponse:
    try:
        page = int(req.params.get('page', 1))
        limit = int(req.params.get('limit', 10))
        search = req.params.get('search', '')
        diet = req.params.get('diet', '')
        
        conn_str = os.environ.get("AzureWebJobsStorage")
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        
        try:
            container_client = blob_service.get_container_client("cleaned-data")
            blob_client = container_client.get_blob_client("cleaned_diets.csv")
            content = blob_client.download_blob().readall()
        except:
            container_client = blob_service.get_container_client("diet-data")
            blob_client = container_client.get_blob_client("All_Diets.csv")
            content = blob_client.download_blob().readall()
        
        df = pd.read_csv(io.BytesIO(content))
        
        if search:
            df = df[df['Recipe_name'].str.contains(search, case=False, na=False)]
        if diet:
            df = df[df['Diet_type'] == diet]
        
        total = len(df)
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        recipes = df.iloc[start_idx:end_idx].to_dict('records')
        
        return func.HttpResponse(
            json.dumps({
                "recipes": recipes,
                "total": total,
                "page": page,
                "limit": limit,
                "total_pages": (total + limit - 1) // limit
            }),
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )