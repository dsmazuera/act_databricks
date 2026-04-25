import argparse
import logging
from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------------------------------------------------------
# 🪵 Logger Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("UTB_Validator")

# ------------------------------------------------------------------------------
# 0️⃣ Parse Arguments
# ------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="UTB Extractor Validator")
parser.add_argument(
    "--source_table",
    required=True,
    help="Spark table name for validation (e.g., logistics.bronze.truckr_loads)"
)
args = parser.parse_args()
source_table = args.source_table

# ------------------------------------------------------------------------------
# 1️⃣ Spark Session
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("UTB_Extractor_Validator").getOrCreate()

# ------------------------------------------------------------------------------
# 2️⃣ Schema Definition
# ------------------------------------------------------------------------------
fields = [
    "source_file", "broker_name", "broker_phone", "broker_fax", "broker_address",
    "broker_city", "broker_state", "broker_zipcode", "broker_email",
    "loadConfirmationNumber", "totalCarrierPay", "carrier_name", "carrier_mc",
    "carrier_address", "carrier_city", "carrier_state", "carrier_zipcode",
    "carrier_phone", "carrier_fax", "carrier_contact", "pickup_customer_1",
    "pickup_address_1", "pickup_city_1", "pickup_state_1", "pickup_zipcode_1",
    "pickup_start_datetime_1", "pickup_end_datetime_1", "delivery_customer_1",
    "delivery_customer_2", "delivery_address_1", "delivery_address_2",
    "delivery_city_1", "delivery_city_2", "delivery_state_1", "delivery_state_2",
    "delivery_zipcode_1", "delivery_zipcode_2", "delivery_start_datetime_1",
    "delivery_start_datetime_2", "delivery_end_datetime_1", "delivery_end_datetime_2"
]
schema = T.StructType([T.StructField(f, T.StringType()) for f in fields])

# ------------------------------------------------------------------------------
# 3️⃣ Ground Truth Record
# ------------------------------------------------------------------------------
truth_record = {
    "source_file": "/mnt/data/1692883297451_CY FL-SC.pdf",
    "broker_name": "Coyote Logistics, LLC",
    "broker_phone": "877-626-9683",
    "broker_fax": "+1 (773) 365 4256",
    "broker_address": "960 Northpoint Parkway Suite 150",
    "broker_city": "Alpharetta",
    "broker_state": "GA",
    "broker_zipcode": "30005",
    "broker_email": "CarrierInvoices@coyote.com; Dan.Matkovic@coyote.com",
    
    "loadConfirmationNumber": "29720595",
    "totalCarrierPay": "800.00",
    
    "carrier_name": "GTT Freight Corp",
    "carrier_mc": "",
    "carrier_address": "",
    "carrier_city": "",
    "carrier_state": "",
    "carrier_zipcode": "",
    "carrier_phone": "",
    "carrier_fax": "",
    "carrier_contact": "Camilo Ramirez",
    
    "pickup_customer_1": "Ocala 3PL 9001",
    "pickup_address_1": "1299 SW 49TH AVE",
    "pickup_city_1": "Ocala",
    "pickup_state_1": "FL",
    "pickup_zipcode_1": "34474",
    "pickup_start_datetime_1": "2023-08-25T08:00:00",
    "pickup_end_datetime_1": "",
    
    "delivery_customer_1": "American Freight Store 216",
    "delivery_customer_2": "American Freight Store 101",
    
    "delivery_address_1": "1680 Richland Ave W",
    "delivery_address_2": "1424 Atlas Rd",
    
    "delivery_city_1": "Aiken",
    "delivery_city_2": "Columbia",
    
    "delivery_state_1": "SC",
    "delivery_state_2": "SC",
    
    "delivery_zipcode_1": "29801",
    "delivery_zipcode_2": "29209",
    
    "delivery_start_datetime_1": "2023-08-25T16:00:00",
    "delivery_start_datetime_2": "2023-08-26T10:00:00",
    
    "delivery_end_datetime_1": "",
    "delivery_end_datetime_2": "",
}
truth_df = spark.createDataFrame([truth_record], schema=schema)
# ------------------------------------------------------------------------------
# 4️⃣ Load Target Table from Parameter
# ------------------------------------------------------------------------------
target_df = spark.table(source_table)

# ------------------------------------------------------------------------------
# 5️⃣ Normalize Data
# ------------------------------------------------------------------------------
def normalize(df):
    string_cols = [c for c, t in df.dtypes if t == "string"]
    return df.select(*[
        F.trim(F.lower(F.col(c))).alias(c) if c in string_cols else F.col(c)
        for c in df.columns
    ])

truth_df = normalize(truth_df)
target_df = normalize(target_df)

# ------------------------------------------------------------------------------
# 6️⃣ Compare Values Field-by-Field
# ------------------------------------------------------------------------------
load_id = truth_record["loadConfirmationNumber"]
target_rows = target_df.filter(F.col("loadConfirmationNumber") == load_id).collect()

results = []

if not target_rows:
    logger.error(f"No record found for loadConfirmationNumber={load_id}")
    for col in schema.fieldNames():
        results.append((col, "❌ Missing record", truth_record.get(col), None))
else:
    logger.info(f"Found record for loadConfirmationNumber={load_id}")
    target_values = target_rows[0].asDict()
    for col in schema.fieldNames():
        truth_val = truth_record.get(col)
        target_val = target_values.get(col)
        norm_truth = str(truth_val).strip().lower() if truth_val else None
        norm_target = str(target_val).strip().lower() if target_val else None
        status = "✅ Match" if norm_truth == norm_target else "❌ Mismatch"
        results.append((col, status, truth_val, target_val))

# ------------------------------------------------------------------------------
# 7️⃣ Log Results
# ------------------------------------------------------------------------------
logger.info(f"Validation results for loadConfirmationNumber={load_id}:")
for field, status, truth, target in results:
    if status == "✅ Match":
        logger.info(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")
    else:
        logger.error(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")

# ------------------------------------------------------------------------------
# 8️⃣ Fail Pipeline if Errors Detected
# ------------------------------------------------------------------------------
errors = [r for r in results if r[1].startswith("❌")]
if errors:
    logger.error(f"Validation failed for {len(errors)} fields")
    for field, status, truth, target in errors:
        logger.error(f"  - {field}: expected='{truth}' got='{target}'")
    raise ValueError(f"Validation failed for {len(errors)} fields")
else:
    logger.info("✅ All fields match perfectly.")
