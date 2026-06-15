# Databricks notebook source
import requests
import time
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row

# COMMAND ----------

def fetch_rest_api_dataset(dataset_name, username="cduuser", password="Rest@1234", per_page=10,date_columns=None):
    import base64
    """
    Fetches ALL pages of a WordPress REST dataset and returns a Spark DataFrame.
    
    Args:
        datasetAC_name (str): API dataset endpoint (e.g., insurance_claims)
        username (str): Basic Auth username
        password (str): Basic Auth password
        per_page (int): Records per page (default 100)
        
    Returns:
        Spark DataFrame
    """
    
    base_url = f"https://cloudanddatauniverse.com/wp-json/custom-api/datasets/{dataset_name}"

    # ---- Build Authorization Header ----
    credentials = f"{username}:{password}"
    token = base64.b64encode(credentials.encode()).decode()
    headers = {"Authorization": f"Basic {token}"}

     # ---- Smart request function (inline) ----
    def make_request(url):
        attempt = 1
        max_attempts = 3

        while attempt <= max_attempts:
            response = requests.get(url, headers=headers)

            try:
                data = response.json()
            except:
                raise Exception(f"Invalid response: {response.text}")

            # ✅ Success
            if response.status_code == 200:
                return data

            # 🚫 Rate limit handling
            if response.status_code == 429 or data.get("code") == "rate_limit_exceeded":

                if attempt == 1:
                    print("⚠️ Rate limit hit. Retrying in 1 minute...")
                    time.sleep(60)

                elif attempt == 2:
                    print("⏳ Still rate limited. Waiting 5 minutes...")
                    time.sleep(300)

                else:
                    raise Exception("❌ Rate limit persists after retries. Failing pipeline.")

                attempt += 1
                continue

            # ❌ Other errors
            raise Exception(f"API Error: {data}")
        
    # ---- Get first page metadata ----
    first_url = f"{base_url}?page=1&per_page={per_page}"
    first_response = make_request(first_url)

    if "total_pages" not in first_response:
        raise Exception(f"Unexpected API response: {first_response}")

    total_pages = first_response["total_pages"]
    per_page = first_response["per_page"]
    print(f"Dataset: {dataset_name} → Total pages = {total_pages}")

    # ---- Loop through all pages ----
    all_rows = []

    for page in range(1, total_pages + 1):
        url = f"{base_url}?page={page}&per_page={per_page}"
        
        response =make_request(url)
        
        if "data" not in response:
            print(f"❌ Error on page {page}: {response}")
            continue

        rows = response["data"]
        all_rows.extend(rows)

        print(f"✔ Fetched page {page}/{total_pages}")

    # ---- Convert to Spark DataFrame ----
    raw_df = spark.createDataFrame(all_rows)
    clean_df = clean_dataset(raw_df)
    typed_df = apply_schema(clean_df,dataset_name)
    return typed_df

# COMMAND ----------

def clean_dataset(df,date_columns=None):

    # 1. Trim strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    # 2. Standardize nulls
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(
                c,
                when(col(c).isin("", " ", "null", "N/A"), None)
                .otherwise(col(c))
            )

    # 3. Remove duplicates
    df = df.dropDuplicates()

    
   # 4. Cast date column
    if date_columns:
        for col_name in date_columns:
            if col_name in df.columns:
                print(f"Casting column : {col_name}")
                df=df.withColumn(col_name, col(col_name).try_cast('timestamp'))

    return df


# COMMAND ----------

schema_registry = {
    
    "insurance_policies": {
        "policy_code": StringType(),
        "name": StringType(),
        "category": StringType(),
        "base_premium_usd": DoubleType(),
        "coverage": StringType(),
        "currency": StringType(),     
        "coverage_type": StringType(),   
        "term_period": StringType(),
        "description": StringType(),
        "start_date":TimestampType(),
        "end_date":TimestampType(),
        "is_active": BooleanType(),
        "_index": LongType()
    },

      "insurance_agents": {
        "agent_id": IntegerType(),
        "agent_name": StringType(),
        "date_of_joining":TimestampType(),
        "experience_years": IntegerType(),  
        "region_id  ": IntegerType(),
        "id_deleted":BooleanType(),
        "agent_email":StringType(),
        "agent_phone":StringType(),
        "agent_gender":StringType(),
        "date_of_birth":StringType(),
        "agent_address":StringType(),
        "city":StringType(),
        "state":StringType(),
        "country":StringType(),
        "agent_type":StringType(),
        "license_number":StringType(),
        "license_expiry_date":StringType(),
        "agent_status":StringType(),
        "total_policies_sold":IntegerType(),
        "total_commission_earned":DecimalType(),
        "rating":DecimalType(),
        "manager_agent_id":IntegerType(),
        "branch_id":IntegerType(),
        "branch_name":StringType(),
        "zone":StringType(),
        "sales_team":StringType(),
        "policies_sold_current_year":IntegerType(),
        "policies_sold_last_year":IntegerType(),
        "avg_policy_value":DecimalType(),
        "conversion_rate":DecimalType(),
        "customer_retention_rate":DecimalType(),
        "commission_rate":DecimalType(),
        "commission_paid_ytd":DecimalType(),
        "commission_pending":DecimalType(),
        "last_commission_date":StringType(),
        "kyc_verified":BooleanType(),
        "background_check_status":StringType(),
        "compliance_score":DecimalType(),
        "last_audit_date":StringType(),
        "last_login_channel":StringType(),
        "login_count_30_days":IntegerType(),
        "last_activity_timestamp":StringType(),
        "device_type":StringType(),
        "avg_response_time_minutes":IntegerType(),
        "complaints_handled":IntegerType(),
        "escalations_count":DecimalType(),
        "customer_satisfaction_score":StringType(),
        "record_created_timestamp":StringType(),
        "record_updated_timestamp":StringType(),
        "batch_id":StringType()
    },
      
    "insurance_customers": {
        "customer_id": IntegerType(),
        "name": StringType(),
        "dob": StringType(),
        "gender": StringType(),
        "occupation": StringType(),
        "address": StringType(),
        "city": StringType(),
        "state": StringType(),
        "country": StringType(),
        "pincode": IntegerType(),
        "email": StringType(),
        "phone": StringType(),
        "Channel": StringType(),
        "nominated": StringType(),
        "nominee_relation": StringType(),
        "start_date": TimestampType(),
        "end_date": TimestampType(),
        "is_active":BooleanType()
    },

    "insurance_countries": {
        "country_id": IntegerType(),
        "country_name": StringType(),
        "_index": LongType()
    },

    "payment_frequency": { 
        "customer_id": IntegerType(),
        "payment_frequency": StringType(),
        "start_date": TimestampType(),
        "end_date": TimestampType(),
        "_index": IntegerType()
    },
    
    "insurance_payments": {
        "customer_id": IntegerType(),
        "policy_id": StringType(),
        "payment_date": TimestampType(),
        "payment_amount": DoubleType(),
        "payment_frequency": StringType(),
        "payment_mode": StringType(),
        "payment_status": StringType(),
        "transaction_id": StringType(),
        "_index": LongType()
    },

    "insurance_claims": {
        "claim_id": StringType(),
        "customer_id": IntegerType(),
        "policy_id": StringType(),
        "claim_date":TimestampType(),
        "incident_date":TimestampType(),
        "claim_amount": DoubleType(),
        "claim_status": StringType(),
        "approval_date": StringType(),
        "settlement_amount": StringType(),
        "fraud_flag": StringType(),
        "channel":StringType(),
        "reported_delay_days":IntegerType(),
        "_index": LongType()
    },

     "customer_policies": {
        "customer_id": IntegerType(),
        "policy_id": StringType(),
        "policy_enroll_date": TimestampType(),
        "_index": LongType()
    },
}


# COMMAND ----------

def apply_schema(df, dataset_name):
    """
    Casts dataframe columns based on schema registry.
    """
    if dataset_name not in schema_registry:
        raise Exception(f"Schema not found for dataset: {dataset_name}")

    schema = schema_registry[dataset_name]

    casted_df = df
    for col_name, col_type in schema.items():
        if col_name in casted_df.columns:
            casted_df = casted_df.withColumn(col_name, col(col_name).cast(col_type))
    
    return casted_df


# COMMAND ----------

def log_pipeline_status(schemaname,tablename,  error_msg):
    schema = StructType([
    StructField("schemaname", StringType(), True),
    StructField("tablename", StringType(), True),
    StructField("error_message", StringType(), True)
])
    log_df = spark.createDataFrame([
    (schemaname, tablename,  error_msg)], schema=schema).withColumn("log_captured",current_timestamp())
    log_df.write.mode("append").saveAsTable("insureallBI.logs.pipelineruns")

# COMMAND ----------

def writeDfToTable(df,schemaname,tablename):
    df.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"insureallBI.{schemaname}.{tablename}")

# COMMAND ----------

def loadIncrementalData(df,schemaName, tableName, mergeKey):
    if not spark.catalog.tableExists(f"insureallBI.{schemaName}.{tableName}"):
        writeDfToTable(df,schemaName,tableName)
    else:
        
        # Build column list for UPDATE and INSERT (excluding merge key as it's in condition)
        columns = [col for col in df.columns if col != mergeKey]
        
        # Build UPDATE SET clause
        update_set_clause = ",\n        ".join([f"target.{col} = source.{col}" for col in columns])
        
        # Build INSERT VALUES clause
        insert_columns = ", ".join(df.columns)
        insert_values = ", ".join([f"source.{col}" for col in df.columns])
        
        # Create temp view
        df.createOrReplaceTempView(f"vw_{tableName}")

        # Execute MERGE statement
        merge_sql = f"""
        MERGE INTO insureallBI.{schemaName}.{tableName} AS target
        USING vw_{tableName} AS source
        ON target.{mergeKey} = source.{mergeKey}
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """
        
        print(merge_sql)

        spark.sql(merge_sql,df=df)
            

# COMMAND ----------

# DBTITLE 1,Auto Loader for Bronze Layer Ingestion
def autoload_to_bronze(
    source_path,
    schema_name,
    table_name,
    file_format="json",
    checkpoint_location=None,
    schema_hints=None,
    merge_schema=True,
    max_files_per_trigger=1000
):
    """
    Auto Loader function to incrementally load data from cloud storage to bronze layer.
    
    Args:
        source_path (str): Cloud storage path (e.g., 's3://bucket/path/', '/Volumes/catalog/schema/volume/')
        schema_name (str): Target schema name in insureallBI catalog
        table_name (str): Target table name
        file_format (str): Source file format - 'json', 'csv', 'parquet', 'avro', 'orc', 'text', 'binaryFile'
        checkpoint_location (str): Optional custom checkpoint location. Defaults to table's _checkpoints path
        schema_hints (dict): Optional schema hints as {"col_name": "data_type"}
        merge_schema (bool): Enable schema evolution (default True)
        max_files_per_trigger (int): Max files to process per trigger (default 1000)
    
    Returns:
        StreamingQuery object
        
    Example:
        autoload_to_bronze(
            source_path="s3://my-bucket/data/claims/",
            schema_name="bronze",
            table_name="insurance_claims_raw",
            file_format="json"
        )
    """
    
    # Set default checkpoint location
    if checkpoint_location is None:
        checkpoint_location = f"/tmp/checkpoints/insureallBI/{schema_name}/{table_name}"
    
    full_table_name = f"insureallBI.{schema_name}.{table_name}"
    
    print(f"🔄 Starting Auto Loader for {full_table_name}")
    print(f"📂 Source: {source_path}")
    print(f"📝 Format: {file_format}")
    print(f"✅ Checkpoint: {checkpoint_location}")
    
    # Build cloudFiles options
    read_options = {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaLocation": checkpoint_location,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns" if merge_schema else "none",
        "cloudFiles.maxFilesPerTrigger": str(max_files_per_trigger)
    }
    
    # Add format-specific options
    if file_format == "csv":
        read_options.update({
            "header": "true",
            "inferSchema": "true",
            "cloudFiles.inferColumnTypes": "true"
        })
    elif file_format == "json":
        read_options["multiLine"] = "true"
    
    # Apply schema hints if provided
    if schema_hints:
        hints_str = ",".join([f"{col} {dtype}" for col, dtype in schema_hints.items()])
        read_options["cloudFiles.schemaHints"] = hints_str
    
    # Read stream using Auto Loader
    df_stream = spark.readStream \
        .format("cloudFiles") \
        .options(**read_options) \
        .load(source_path)
    
    # Add metadata columns for tracking
    df_stream = df_stream \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_file", col("_metadata.file_path")) \
        .withColumn("_source_file_modification_time", col("_metadata.file_modification_time"))
    
    # Write to bronze Delta table
    query = df_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .option("mergeSchema", "true" if merge_schema else "false") \
        .trigger(availableNow=True) \
        .toTable(full_table_name)
    
    # Wait for completion
    query.awaitTermination()
    
    print(f"✅ Auto Loader completed for {full_table_name}")
    
    # Log success
    try:
        log_pipeline_status(schema_name, table_name, "Auto Loader completed successfully")
    except Exception as log_error:
        print(f"⚠️ Logging failed: {log_error}")
    
    return query
