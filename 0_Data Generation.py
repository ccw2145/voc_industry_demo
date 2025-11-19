# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Inn VoC Demo: Data Gen 
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "voc_demo")
dbutils.widgets.text("review_table", "raw_reviews")


# COMMAND ----------

# Set the default catalog and schema to Unity Catalog
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Hotel Location Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create temporary table with all 120 locations and their coordinates
# MAGIC CREATE TABLE IF NOT EXISTS hotel_locations AS
# MAGIC SELECT location, latitude, longitude FROM (VALUES
# MAGIC     -- Northeast (20)
# MAGIC     ('Boston, MA', 42.3601, -71.0589),
# MAGIC     ('Cambridge, MA', 42.3736, -71.1097),
# MAGIC     ('Worcester, MA', 42.2626, -71.8023),
# MAGIC     ('Springfield, MA', 42.1015, -72.5898),
# MAGIC     ('Providence, RI', 41.8240, -71.4128),
# MAGIC     ('Hartford, CT', 41.7658, -72.6734),
# MAGIC     ('New Haven, CT', 41.3083, -72.9279),
# MAGIC     ('Stamford, CT', 41.0534, -73.5387),
# MAGIC     ('Portland, ME', 43.6591, -70.2568),
# MAGIC     ('Bangor, ME', 44.8012, -68.7778),
# MAGIC     ('Manchester, NH', 42.9956, -71.4548),
# MAGIC     ('Concord, NH', 43.2081, -71.5376),
# MAGIC     ('Burlington, VT', 44.4759, -73.2121),
# MAGIC     ('Albany, NY', 42.6526, -73.7562),
# MAGIC     ('Syracuse, NY', 43.0481, -76.1474),
# MAGIC     ('Rochester, NY', 43.1566, -77.6088),
# MAGIC     ('Buffalo, NY', 42.8864, -78.8784),
# MAGIC     ('White Plains, NY', 41.0340, -73.7629),
# MAGIC     ('Newark, NJ', 40.7357, -74.1724),
# MAGIC     ('Jersey City, NJ', 40.7178, -74.0431),
# MAGIC     
# MAGIC     -- Mid-Atlantic (20)
# MAGIC     ('New York City, NY (Manhattan)', 40.7831, -73.9712),
# MAGIC     ('Brooklyn, NY', 40.6782, -73.9442),
# MAGIC     ('Queens, NY', 40.7282, -73.7949),
# MAGIC     ('Long Island, NY', 40.7891, -73.1350),
# MAGIC     ('Atlantic City, NJ', 39.3643, -74.4229),
# MAGIC     ('Philadelphia, PA', 39.9526, -75.1652),
# MAGIC     ('Pittsburgh, PA', 40.4406, -79.9959),
# MAGIC     ('Harrisburg, PA', 40.2732, -76.8867),
# MAGIC     ('Allentown, PA', 40.6084, -75.4902),
# MAGIC     ('Wilmington, DE', 39.7391, -75.5398),
# MAGIC     ('Baltimore, MD', 39.2904, -76.6122),
# MAGIC     ('Annapolis, MD', 38.9784, -76.4922),
# MAGIC     ('Silver Spring, MD', 38.9907, -77.0261),
# MAGIC     ('Washington, DC', 38.9072, -77.0369),
# MAGIC     ('Arlington, VA', 38.8816, -77.0910),
# MAGIC     ('Alexandria, VA', 38.8048, -77.0469),
# MAGIC     ('Richmond, VA', 37.5407, -77.4360),
# MAGIC     ('Virginia Beach, VA', 36.8529, -75.9780),
# MAGIC     ('Norfolk, VA', 36.8508, -76.2859),
# MAGIC     ('Charlottesville, VA', 38.0293, -78.4767),
# MAGIC     
# MAGIC     -- Southeast (20)
# MAGIC     ('Charlotte, NC', 35.2271, -80.8431),
# MAGIC     ('Raleigh, NC', 35.7796, -78.6382),
# MAGIC     ('Durham, NC', 35.9940, -78.8986),
# MAGIC     ('Greensboro, NC', 36.0726, -79.7920),
# MAGIC     ('Asheville, NC', 35.5951, -82.5515),
# MAGIC     ('Charleston, SC', 32.7765, -79.9311),
# MAGIC     ('Columbia, SC', 34.0007, -81.0348),
# MAGIC     ('Greenville, SC', 34.8526, -82.3940),
# MAGIC     ('Atlanta, GA', 33.7490, -84.3880),
# MAGIC     ('Savannah, GA', 32.0809, -81.0912),
# MAGIC     ('Augusta, GA', 33.4735, -82.0105),
# MAGIC     ('Orlando, FL', 28.5383, -81.3792),
# MAGIC     ('Tampa, FL', 27.9506, -82.4572),
# MAGIC     ('Miami, FL', 25.7617, -80.1918),
# MAGIC     ('Fort Lauderdale, FL', 26.1224, -80.1373),
# MAGIC     ('West Palm Beach, FL', 26.7153, -80.0534),
# MAGIC     ('Jacksonville, FL', 30.3322, -81.6557),
# MAGIC     ('Tallahassee, FL', 30.4383, -84.2807),
# MAGIC     ('Pensacola, FL', 30.4213, -87.2169),
# MAGIC     ('Key West, FL', 24.5551, -81.7800),
# MAGIC     
# MAGIC     -- Midwest (20)
# MAGIC     ('Chicago, IL', 41.8781, -87.6298),
# MAGIC     ('Naperville, IL', 41.7508, -88.1535),
# MAGIC     ('Springfield, IL', 39.7817, -89.6501),
# MAGIC     ('Rockford, IL', 42.2711, -89.0940),
# MAGIC     ('Indianapolis, IN', 39.7684, -86.1581),
# MAGIC     ('Fort Wayne, IN', 41.0793, -85.1394),
# MAGIC     ('Columbus, OH', 39.9612, -82.9988),
# MAGIC     ('Cleveland, OH', 41.4993, -81.6944),
# MAGIC     ('Cincinnati, OH', 39.1031, -84.5120),
# MAGIC     ('Toledo, OH', 41.6528, -83.5379),
# MAGIC     ('Detroit, MI', 42.3314, -83.0458),
# MAGIC     ('Ann Arbor, MI', 42.2808, -83.7430),
# MAGIC     ('Grand Rapids, MI', 42.9634, -85.6681),
# MAGIC     ('Milwaukee, WI', 43.0389, -87.9065),
# MAGIC     ('Madison, WI', 43.0731, -89.4012),
# MAGIC     ('Green Bay, WI', 44.5133, -88.0133),
# MAGIC     ('Minneapolis, MN', 44.9778, -93.2650),
# MAGIC     ('St. Paul, MN', 44.9537, -93.0900),
# MAGIC     ('Des Moines, IA', 41.6005, -93.6091),
# MAGIC     ('Kansas City, MO', 39.0997, -94.5786),
# MAGIC     
# MAGIC     -- South (15)
# MAGIC     ('Nashville, TN', 36.1627, -86.7816),
# MAGIC     ('Memphis, TN', 35.1495, -90.0490),
# MAGIC     ('Knoxville, TN', 35.9606, -83.9207),
# MAGIC     ('Chattanooga, TN', 35.0456, -85.3097),
# MAGIC     ('Louisville, KY', 38.2527, -85.7585),
# MAGIC     ('Lexington, KY', 38.0406, -84.5037),
# MAGIC     ('Birmingham, AL', 33.5207, -86.8025),
# MAGIC     ('Montgomery, AL', 32.3668, -86.3000),
# MAGIC     ('Huntsville, AL', 34.7304, -86.5861),
# MAGIC     ('New Orleans, LA', 29.9511, -90.0715),
# MAGIC     ('Baton Rouge, LA', 30.4515, -91.1871),
# MAGIC     ('Little Rock, AR', 34.7465, -92.2896),
# MAGIC     ('Fayetteville, AR', 36.0626, -94.1574),
# MAGIC     ('Oklahoma City, OK', 35.4676, -97.5164),
# MAGIC     ('Tulsa, OK', 36.1540, -95.9928),
# MAGIC     
# MAGIC     -- Southwest (10)
# MAGIC     ('Dallas, TX', 32.7767, -96.7970),
# MAGIC     ('Fort Worth, TX', 32.7555, -97.3308),
# MAGIC     ('Houston, TX', 29.7604, -95.3698),
# MAGIC     ('Austin, TX', 30.2672, -97.7431),
# MAGIC     ('San Antonio, TX', 29.4241, -98.4936),
# MAGIC     ('El Paso, TX', 31.7619, -106.4850),
# MAGIC     ('Albuquerque, NM', 35.0844, -106.6504),
# MAGIC     ('Santa Fe, NM', 35.6870, -105.9378),
# MAGIC     ('Phoenix, AZ', 33.4484, -112.0740),
# MAGIC     ('Tucson, AZ', 32.2226, -110.9747),
# MAGIC     
# MAGIC     -- West (15)
# MAGIC     ('Denver, CO', 39.7392, -104.9903),
# MAGIC     ('Colorado Springs, CO', 38.8339, -104.8214),
# MAGIC     ('Salt Lake City, UT', 40.7608, -111.8910),
# MAGIC     ('Park City, UT', 40.6461, -111.4980),
# MAGIC     ('Las Vegas, NV', 36.1699, -115.1398),
# MAGIC     ('Reno, NV', 39.5296, -119.8138),
# MAGIC     ('Boise, ID', 43.6150, -116.2023),
# MAGIC     ('Spokane, WA', 47.6588, -117.4260),
# MAGIC     ('Seattle, WA', 47.6062, -122.3321),
# MAGIC     ('Tacoma, WA', 47.2529, -122.4443),
# MAGIC     ('Portland, OR', 45.5152, -122.6784),
# MAGIC     ('Eugene, OR', 44.0521, -123.0868),
# MAGIC     ('San Diego, CA', 32.7157, -117.1611),
# MAGIC     ('Los Angeles, CA', 34.0522, -118.2437),
# MAGIC     ('San Francisco, CA', 37.7749, -122.4194)
# MAGIC ) AS locations(location, latitude, longitude);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table hotel_locations as
# MAGIC select 
# MAGIC   *,
# MAGIC   case
# MAGIC     when location in (
# MAGIC       'Boston, MA', 'Cambridge, MA', 'Worcester, MA', 'Springfield, MA', 'Providence, RI', 'Hartford, CT', 'New Haven, CT', 'Stamford, CT', 'Portland, ME', 'Bangor, ME', 'Manchester, NH', 'Concord, NH', 'Burlington, VT', 'Albany, NY', 'Syracuse, NY', 'Rochester, NY', 'Buffalo, NY', 'White Plains, NY', 'Newark, NJ', 'Jersey City, NJ'
# MAGIC     ) then 'Northeast'
# MAGIC     when location in (
# MAGIC       'New York City, NY (Manhattan)', 'Brooklyn, NY', 'Queens, NY', 'Long Island, NY', 'Atlantic City, NJ', 'Philadelphia, PA', 'Pittsburgh, PA', 'Harrisburg, PA', 'Allentown, PA', 'Wilmington, DE', 'Baltimore, MD', 'Annapolis, MD', 'Silver Spring, MD', 'Washington, DC', 'Arlington, VA', 'Alexandria, VA', 'Richmond, VA', 'Virginia Beach, VA', 'Norfolk, VA', 'Charlottesville, VA'
# MAGIC     ) then 'Mid-Atlantic'
# MAGIC     when location in (
# MAGIC       'Charlotte, NC', 'Raleigh, NC', 'Durham, NC', 'Greensboro, NC', 'Asheville, NC', 'Charleston, SC', 'Columbia, SC', 'Greenville, SC', 'Atlanta, GA', 'Savannah, GA', 'Augusta, GA', 'Orlando, FL', 'Tampa, FL', 'Miami, FL', 'Fort Lauderdale, FL', 'West Palm Beach, FL', 'Jacksonville, FL', 'Tallahassee, FL', 'Pensacola, FL', 'Key West, FL'
# MAGIC     ) then 'Southeast'
# MAGIC     when location in (
# MAGIC       'Chicago, IL', 'Naperville, IL', 'Springfield, IL', 'Rockford, IL', 'Indianapolis, IN', 'Fort Wayne, IN', 'Columbus, OH', 'Cleveland, OH', 'Cincinnati, OH', 'Toledo, OH', 'Detroit, MI', 'Ann Arbor, MI', 'Grand Rapids, MI', 'Milwaukee, WI', 'Madison, WI', 'Green Bay, WI', 'Minneapolis, MN', 'St. Paul, MN', 'Des Moines, IA', 'Kansas City, MO'
# MAGIC     ) then 'Midwest'
# MAGIC     when location in (
# MAGIC       'Nashville, TN', 'Memphis, TN', 'Knoxville, TN', 'Chattanooga, TN', 'Louisville, KY', 'Lexington, KY', 'Birmingham, AL', 'Montgomery, AL', 'Huntsville, AL', 'New Orleans, LA', 'Baton Rouge, LA', 'Little Rock, AR', 'Fayetteville, AR', 'Oklahoma City, OK', 'Tulsa, OK'
# MAGIC     ) then 'South'
# MAGIC     when location in (
# MAGIC       'Dallas, TX', 'Fort Worth, TX', 'Houston, TX', 'Austin, TX', 'San Antonio, TX', 'El Paso, TX', 'Albuquerque, NM', 'Santa Fe, NM', 'Phoenix, AZ', 'Tucson, AZ'
# MAGIC     ) then 'Southwest'
# MAGIC     when location in (
# MAGIC       'Denver, CO', 'Colorado Springs, CO', 'Salt Lake City, UT', 'Park City, UT', 'Las Vegas, NV', 'Reno, NV', 'Boise, ID', 'Spokane, WA', 'Seattle, WA', 'Tacoma, WA', 'Portland, OR', 'Eugene, OR', 'San Diego, CA', 'Los Angeles, CA', 'San Francisco, CA'
# MAGIC     ) then 'West'
# MAGIC     else null
# MAGIC   end as region
# MAGIC from hotel_locations;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE hotel_locations SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Hotel Reviews

# COMMAND ----------

from pyspark.sql import functions as F

NUM_REVIEWS = 1000
MODEL = "databricks-meta-llama-3-3-70b-instruct"
table_name = dbutils.widgets.get("table_name")

hl = spark.table("hotel_locations")

df = (
    spark.range(NUM_REVIEWS)
    .join(F.broadcast(hl.sample(withReplacement=True, fraction=1.0)), how="cross")
    .limit(NUM_REVIEWS)
)

# Random weighted fields
df = (
    df
    .withColumn("review_uid",F.expr("uuid()"))
    .withColumn(
        "channel",
        F.when(F.rand() < 0.25, "Google")
         .when(F.rand() < 0.45, "TripAdvisor")
         .when(F.rand() < 0.65, "Booking.com")
         .when(F.rand() < 0.80, "Expedia")
         .when(F.rand() < 0.90, "Hotels.com")
         .otherwise("Yelp")
    )
    .withColumn(
        "star_rating",
        F.when(F.rand() < 0.35, 5)
         .when(F.rand() < 0.60, 4)
         .when(F.rand() < 0.75, 3)
         .when(F.rand() < 0.95, 2)
         .otherwise(1)
         .cast("bigint")
    )
    .withColumn("review_date", F.date_sub(F.current_date(), (F.rand() * 180).cast("int")))
    .withColumnRenamed("loc", "location")
    .withColumnRenamed("lng", "longitude")
    .withColumnRenamed("lat", "latitude")
    .drop('id')
)

# Add LLM-generated review text
df = df.withColumn(
    "review_text",
    F.expr(f"""
      ai_query(
        '{MODEL}',
        'Generate a realistic hotel review (max 200 words) for a customer who stayed at '
          || location ||
          ', rated their experience ' || CAST(star_rating AS STRING) ||
          ' stars via ' || channel ||
          ' on ' || CAST(review_date AS STRING) || '.'
      )
    """)
)

# Save
df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")
display(df.limit(10))

# COMMAND ----------

df.toPandas()

# COMMAND ----------

# %sql
# -- Generate 5000 hotel reviews
# INSERT INTO raw_reviews
# SELECT 
#     CONCAT(LPAD(CAST(id AS STRING), 6, '0')) as review_id,
    
#     -- Select random location from hotel_locations
#     hl.location,
    
#     -- Random review channel with weighted distribution
#     CASE 
#         WHEN rand() < 0.25 THEN 'Google'
#         WHEN rand() < 0.45 THEN 'TripAdvisor'
#         WHEN rand() < 0.65 THEN 'Booking.com'
#         WHEN rand() < 0.80 THEN 'Expedia'
#         WHEN rand() < 0.90 THEN 'Hotels.com'
#         ELSE 'Yelp'
#     END as channel,
    
#     -- Star rating with realistic distribution (more 4-5 stars)
#     CASE 
#         WHEN rand() < 0.35 THEN 5
#         WHEN rand() < 0.60 THEN 4
#         WHEN rand() < 0.75 THEN 3
#         WHEN rand() < 0.95 THEN 2
#         ELSE 1
#     END as star_rating,
    
#     -- Random date within last 6 months (180 days)
#     DATE_SUB(CURRENT_DATE(), CAST(rand() * 180 AS INT)) as review_date,
    
#     -- Use coordinates from hotel_locations
#     hl.longitude,
#     hl.latitude

# FROM (SELECT id FROM RANGE(5000)) r
# JOIN (
#     SELECT 
#         location, 
#         longitude, 
#         latitude,
#         ROW_NUMBER() OVER (ORDER BY rand()) as rn
#     FROM hotel_locations
# ) hl ON MOD(r.id, (SELECT COUNT(*) FROM hotel_locations)) = MOD(hl.rn - 1, (SELECT COUNT(*) FROM hotel_locations));

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# %sql
# -- Generate the AI review text in a temp view
# CREATE OR REPLACE TEMP VIEW tmp_reviews AS
# SELECT
#   review_id,
#   ai_query(
#     "databricks-meta-llama-3-3-70b-instruct",
#     'Generate a realistic hotel review (max 200 words) for a customer who stayed at ' || location ||
#     ', rated their experience ' || star_rating || ' stars via ' || channel ||
#     ' on ' || CAST(review_date AS STRING) || '.'
#   ) AS review_text
# FROM raw_reviews;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Runbook for each aspect

# COMMAND ----------

# --- 1) Define aspects + descriptions (exact keys) ---
aspects = {
    
    # Arrival & Departure
    "check_in_out": "Front desk or kiosk experience, wait times, early/late checkout, key card issuance",
    "parking": "Parking availability, ease of access, pricing, safety, EV charging availability",
    "booking_reservation": "Booking via website, app, or OTA; overbooking; handling of special requests",

    # Staff & Service
    "staff_friendliness": "Courtesy, professionalism, helpfulness, attentiveness, recognition of guests",
    "loyalty_experience": "Loyalty program recognition, upgrades, perks, point redemption or handling",
    "safety_security": "Personal safety, theft, security staff, door locks, billing security, fraud concerns",

    # In-Room Experience
    "cleanliness": "Room and common area cleanliness, housekeeping quality and thoroughness",
    "room_quality": "Room size, layout, furnishings, lighting, outlets, in-room technology (excluding cleanliness)",
    "bathroom_quality": "Bathroom fixtures, water temperature/pressure, shower/tub, toiletries, plumbing reliability",
    "bed_sleep_quality": "Mattress, pillows, linens, bed comfort, blackout curtains, ability to sleep well",
    "temperature_hvac": "Heating, cooling, ventilation, thermostat accuracy, AC/heater noise",
    "wifi_connectivity": "Internet speed, reliability, login process, free vs paid access",

    # Food & Beverage
    "breakfast_quality": "Breakfast options, variety, freshness, hours, buffet line experience",
    "fnb_other": "On-site restaurant, bar, café, or room service quality (excluding breakfast)",

    # Facilities & Amenities
    "maintenance": "Broken or out-of-order equipment, outages, elevator/kiosk/key card malfunctions",
    "amenities_fitness": "Gym equipment availability, condition, cleanliness, and operating hours",
    "pool_spa": "Pool, hot tub, sauna, or spa cleanliness, temperature, availability, or closures",

    # Environment & Location
    "noise_ambience": "Noise from hallways, elevators, neighbors, or street; overall atmosphere and vibe",
    "location_convenience": "Proximity to transit, airport, attractions, or restaurants; neighborhood quality",

    # Value & Loyalty
    "value_price": "Price fairness, hidden fees, value for money compared to expectations"
}

# COMMAND ----------

import json
from pyspark.sql import functions as F, types as T

# --- 2) Build prompt (JSON-embed safely) ---
aspect_value_schema = {
    "type": "object",
    "properties": {
        "action": {"type": "string"},
        "action_items": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 5,
            "maxItems": 5
        },
        "expected_impact": {"type": "string"},
        "timeline": {"type": "string"},
        "cost_estimate": {
            "type": "string",
            "enum": ["Low", "Medium", "High", "Low-Medium", "Medium-High"]
        },
        "difficulty": {
            "type": "string",
            "enum": ["Low", "Medium", "High"]
        }
    },
    "required": [
        "action", "action_items", "expected_impact",
        "timeline", "cost_estimate", "difficulty"
    ],
    "additionalProperties": False
}

# Top-level object whose keys are EXACTLY your aspect names
json_schema = {
    "name": "hotel_remediation_plans",
    "schema": {
        "type": "object",
        "properties": {k: aspect_value_schema for k in aspects.keys()},
        "required": list(aspects.keys()),
        "additionalProperties": False
    },
    # 'strict' asks the model to adhere tightly to the schema
    "strict": True
}

response_format = {
    "type": "json_schema",
    "json_schema": json_schema
}

# 3) Build the prompt (include descriptions to tailor content)
model_name = "databricks-claude-sonnet-4"  
prompt = f"""You are generating remediation plans for hotel operations.

Use the following aspects and descriptions (JSON map of aspect_name -> description):
{json.dumps(aspects, ensure_ascii=False)}

For each aspect, provide:
- action: concise, imperative
- action_items: EXACTLY 5 crisp, actionable bullet points
- expected_impact: include a % and a timeframe (e.g., "Reduce X by 40% within 3 weeks")
- timeline: short (e.g., "2 weeks")
- cost_estimate: one of [Low, Medium, High, Low-Medium, Medium-High]
- difficulty: one of [Low, Medium, High]

Return ONLY a JSON object keyed by the exact aspect names (snake_case)."""

# SQL-literal escaping
prompt_sql = prompt.replace("'", "''")
response_format_sql = json.dumps(response_format).replace("'", "''")

df_out = spark.sql(f"""
SELECT ai_query(
  '{model_name}',
  '{prompt_sql}',
  responseFormat => '{response_format_sql}',
  failOnError   => false
) AS out
""")

# --- 2) Extract and parse fields ---
df_parsed = (
    df_out
    .select(
        F.col("out.result").alias("raw_response"),   # raw JSON
        F.col("out.errorMessage").alias("error_status")
    )
)

# Define schema for parsed JSON
spark_schema = T.MapType(
    T.StringType(),
    T.StructType([
        T.StructField("action", T.StringType()),
        T.StructField("action_items", T.ArrayType(T.StringType())),
        T.StructField("expected_impact", T.StringType()),
        T.StructField("timeline", T.StringType()),
        T.StructField("cost_estimate", T.StringType()),
        T.StructField("difficulty", T.StringType()),
    ])
)

# Parse JSON + explode into rows
plans_df = (
    df_parsed
    .withColumn("parsed_map", F.from_json("raw_response", spark_schema))
    .selectExpr("explode(parsed_map) as (aspect, plan)", "raw_response", "error_status")
    .select(
        # "raw_response",                # keep raw JSON
        "aspect",
        "plan.action",
        "plan.action_items",
        "plan.expected_impact",
        "plan.timeline",
        "plan.cost_estimate",
        "plan.difficulty",
        "error_status"
    )
)

# COMMAND ----------

# Drop the table if it exists in Unity Catalog
spark.sql(
    """
    DROP TABLE IF EXISTS lakehouse_inn_catalog.voc.aspect_runbook
    """
)

# Overwrite the table in Unity Catalog
plans_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(
    "lakehouse_inn_catalog.voc.aspect_runbook"
)

# COMMAND ----------

display(spark.table("lakehouse_inn_catalog.voc.aspect_runbook"))

# COMMAND ----------

examples = """
'Room Cleanliness': {
                'action': 'Implement additional housekeeping quality checks and provide refresher training on deep cleaning protocols.',
                'action_items': [
                    'Schedule immediate housekeeping audit of all rooms',
                    'Review and replenish cleaning supply inventory',
                    'Implement guest room inspection checklist',
                    'Provide deep cleaning protocol training to housekeeping staff',
                    'Install quality control checkpoints in cleaning process'
                ],
                'expected_impact': 'Reduce negative cleanliness reviews by 60% within 2 weeks',
                'timeline': '2 weeks',
                'cost_estimate': 'Low-Medium',
                'difficulty': 'Medium'
            },
            'Staff Service': {
                'action': 'Enhance customer service training and implement guest interaction protocols.',
                'action_items': [
                    'Conduct comprehensive staff service training workshop',
                    'Implement standardized guest greeting procedures',
                    'Review and optimize response time protocols',
                    'Create guest service excellence guidelines',
                    'Establish regular service quality assessments'
                ],
                'expected_impact': 'Improve staff service ratings by 40% within 3 weeks',
                'timeline': '3 weeks',
                'cost_estimate': 'Low',
                'difficulty': 'Medium'
            },
            'WiFi Connectivity': {
                'action': 'Upgrade network infrastructure and implement redundant internet connections.',
                'action_items': [
                    'Upgrade to enterprise-grade WiFi equipment',
                    'Add backup internet service provider',
                    'Implement comprehensive network monitoring system',
                    'Conduct WiFi coverage analysis and optimization',
                    'Create guest network troubleshooting procedures'
                ],
                'expected_impact': 'Eliminate WiFi connectivity issues within 1 week',
                'timeline': '1 week',
                'cost_estimate': 'High',
                'difficulty': 'High'
            },
            'Noise Levels': {
                'action': 'Implement noise reduction measures and review room soundproofing.',
                'action_items': [
                    'Install additional soundproofing materials in affected rooms',
                    'Review and service HVAC systems for noise reduction',
                    'Implement and enforce quiet hours policy',
                    'Inspect and improve room-to-room sound isolation',
                    'Create noise complaint response procedures'
                ],
                'expected_impact': 'Reduce noise complaints by 50% within 4 weeks',
                'timeline': '4 weeks',
                'cost_estimate': 'Medium-High',
                'difficulty': 'High'
            },
            'Amenities': {
                'action': 'Audit and upgrade guest amenities based on feedback analysis.',
                'action_items': [
                    'Conduct comprehensive amenities audit',
                    'Review maintenance schedules for all facilities',
                    'Update amenity offerings based on guest preferences',
                    'Ensure consistent amenity availability and quality',
                    'Implement amenity feedback collection system'
                ],
                'expected_impact': 'Improve amenity satisfaction by 35% within 3 weeks',
                'timeline': '3 weeks',
                'cost_estimate': 'Medium',
                'difficulty': 'Medium'
            }
        }
"""

# COMMAND ----------


