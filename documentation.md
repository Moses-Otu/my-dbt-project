# Environment Setup
---
## Snowflake user creation
---

```sql
-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='xxxxxxx'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='AIRBNB.RAW'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM to USER dbt;

-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS AIRBNB;
CREATE SCHEMA IF NOT EXISTS AIRBNB.RAW;

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM; 
GRANT ALL ON DATABASE AIRBNB to ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE AIRBNB to ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE AIRBNB to ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA AIRBNB.RAW to ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA AIRBNB.RAW to ROLE TRANSFORM;
```
## Snowflake data import (python scripts for batch loading)
---
copy code and convert to IPYNB
``` sql
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "7370ad94-07f5-4dd6-9cb8-b6461ec61ccf",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import random\n",
    "from datetime import datetime, timedelta"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d8315c34-9cf7-4c00-b2ae-316d1e3f552c",
   "metadata": {},
   "outputs": [],
   "source": [
    "pip install pandas pyarrow"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "993c21b9-e452-4ced-a1e9-f3cf757d693d",
   "metadata": {},
   "outputs": [],
   "source": [
    "%pip install snowflake-snowpark-python"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "91bb50ee-ae65-4898-962d-e14de2734996",
   "metadata": {},
   "outputs": [],
   "source": [
    "!pip install \"snowflake-connector-python[pandas]\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c725009a-5cb2-4214-8408-0ceac3c665f6",
   "metadata": {},
   "outputs": [],
   "source": [
    "from snowflake.connector.pandas_tools import write_pandas"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "61487325-a2f9-411d-855c-258b08888db4",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "## GENERATE FILE FROM FAKER\n",
    "\n",
    "import pandas as pd\n",
    "import random\n",
    "from datetime import datetime, timedelta\n",
    "\n",
    "# Configuration\n",
    "TOTAL_ROWS = 5500000\n",
    "BATCH_SIZE = 500000\n",
    "\n",
    "# Seed for reproducibility\n",
    "random.seed(42)\n",
    "\n",
    "# Sample data generators\n",
    "ROOM_TYPES = [\"Entire home/apt\", \"Private room\", \"Shared room\", \"Hotel room\"]\n",
    "SENTIMENTS = [\"Positive\", \"Negative\", \"Neutral\"]\n",
    "FIRST_NAMES = [\"John\", \"Jane\", \"Robert\", \"Emily\", \"Michael\", \"Sarah\", \"David\", \"Lisa\"]\n",
    "LAST_NAMES = [\"Smith\", \"Johnson\", \"Williams\", \"Brown\", \"Jones\", \"Miller\", \"Davis\", \"Garcia\"]\n",
    "PROPERTY_ADJECTIVES = [\"Cozy\", \"Modern\", \"Luxury\", \"Charming\", \"Spacious\", \"Bright\", \"Stylish\"]\n",
    "PROPERTY_NOUNS = [\"Apartment\", \"House\", \"Loft\", \"Cottage\", \"Villa\", \"Studio\", \"Flat\"]\n",
    "REVIEW_COMMENTS = [\n",
    "    \"Great place to stay!\", \"Had a wonderful time.\", \"Could be better.\", \"Perfect location!\",\n",
    "    \"The host was very helpful.\", \"Needs some maintenance.\", \"Clean and comfortable.\",\n",
    "    \"Would stay here again.\", \"Not as described.\", \"Excellent experience overall.\"\n",
    "]\n",
    "\n",
    "def generate_date(start_date, end_date):\n",
    "    time_between = end_date - start_date\n",
    "    random_days = random.randrange(time_between.days)\n",
    "    return start_date + timedelta(days=random_days)\n",
    "\n",
    "def generate_large_dataset(total_rows, batch_size):\n",
    "    # Columns for the DataFrame\n",
    "    columns = [\n",
    "        \"fact_id\", \"listing_id\", \"listing_url\", \"name\", \"room_type\", \"minimum_nights\", \n",
    "        \"host_id\", \"host_name\", \"is_superhost\", \"price\", \"review_date\", \n",
    "        \"reviewer_name\", \"review_comments\", \"sentiment\", \"created_at\", \"updated_at\"\n",
    "    ]\n",
    "    \n",
    "    # Open CSV file for writing\n",
    "    with open('airbnb_fact_data.csv', 'w') as csv_file:\n",
    "        # Write header\n",
    "        csv_file.write(','.join(columns) + '\\n')\n",
    "        \n",
    "        # Generate data in batches\n",
    "        for batch_num in range(0, total_rows, batch_size):\n",
    "            current_batch_size = min(batch_size, total_rows - batch_num)\n",
    "            print(f\"Generating batch starting at row {batch_num}, size {current_batch_size}\")\n",
    "            \n",
    "            # Generate and write batch\n",
    "            for i in range(current_batch_size):\n",
    "                fact_id = batch_num + i\n",
    "                current_time = datetime.now()\n",
    "                listing_id = random.randint(1000, 999999)\n",
    "                \n",
    "                # Prepare row data\n",
    "                row_data = [\n",
    "                    fact_id,  # fact_id\n",
    "                    listing_id,  # listing_id\n",
    "                    f\"https://example.com/listings/{listing_id}\",  # listing_url\n",
    "                    f\"{random.choice(PROPERTY_ADJECTIVES)} {random.choice(PROPERTY_NOUNS)}\",  # name\n",
    "                    random.choice(ROOM_TYPES),  # room_type\n",
    "                    random.randint(1, 30),  # minimum_nights\n",
    "                    random.randint(1000, 99999),  # host_id\n",
    "                    f\"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}\",  # host_name\n",
    "                    random.choice([\"true\", \"false\"]),  # is_superhost\n",
    "                    round(random.uniform(50, 500), 2),  # price\n",
    "                    generate_date(current_time - timedelta(days=730), current_time),  # review_date\n",
    "                    f\"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}\",  # reviewer_name\n",
    "                    random.choice(REVIEW_COMMENTS),  # review_comments\n",
    "                    random.choice(SENTIMENTS),  # sentiment\n",
    "                    current_time,  # created_at\n",
    "                    current_time  # updated_at\n",
    "                ]\n",
    "                \n",
    "                # Convert to CSV line\n",
    "                csv_line = ','.join(map(str, row_data)) + '\\n'\n",
    "                csv_file.write(csv_line)\n",
    "        \n",
    "        print(\"\\nData generation completed!\")\n",
    "        print(f\"Total rows: {total_rows}\")\n",
    "\n",
    "# Execute data generation\n",
    "generate_large_dataset(TOTAL_ROWS, BATCH_SIZE)\n",
    "\n",
    "# Verify file\n",
    "import os\n",
    "print(f\"\\nFile size: {os.path.getsize('airbnb_fact_data.csv') / (1024 * 1024):.2f} MB\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "4e2c8003-1e35-4c4c-9abe-63ce897ffd99",
   "metadata": {},
   "outputs": [],
   "source": [
    "## Import as df\n",
    "\n",
    "import pandas as pd\n",
    "\n",
    "# Define the local file path\n",
    "file_path = r\"C:\\Users\\crypt\\airbnb_fact_data.csv\"  # Use raw string (r\"\") to avoid escape issues\n",
    "\n",
    "# Read the CSV into a DataFrame\n",
    "df = pd.read_csv(file_path)\n",
    "\n",
    "# Display first few rows\n",
    "print(\"✅ Data Loaded Successfully!\")\n",
    "print(df.head())\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "56088b91-1672-4afd-ac19-43d5120d66c8",
   "metadata": {},
   "outputs": [],
   "source": [
    "import snowflake.connector\n",
    "\n",
    "conn = snowflake.connector.connect(\n",
    "    user='bills',\n",
    "    password='@567Otimosesotu',##use your original pasword for snowflakes\n",
    "    account='fwjrpil-rb14670',\n",
    "    warehouse='COMPUTE_WH',\n",
    "    database='AIRBNB',\n",
    "    schema='RAW',\n",
    "    role='ACCOUNTADMIN'\n",
    "    )\n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "fbc7d19c-ea8a-4a56-9624-33f3739f292f",
   "metadata": {},
   "outputs": [],
   "source": [
    "curr=conn.cursor()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c0758d59-d149-4831-ab06-aa80f47614f5",
   "metadata": {},
   "outputs": [],
   "source": [
    "curr.execute(\n",
    "    'CREATE TABLE \"airbnb_tx_table1\" ('\n",
    "    '\"fact_id\" INT, '\n",
    "    '\"listing_id\" INT, '\n",
    "    '\"listing_url\" STRING, '\n",
    "    '\"name\" STRING, '\n",
    "    '\"room_type\" STRING, '\n",
    "    '\"minimum_nights\" INT, '\n",
    "    '\"host_id\" INT, '\n",
    "    '\"host_name\" STRING, '\n",
    "    '\"is_superhost\" STRING, '\n",
    "    '\"price\" FLOAT, '\n",
    "    '\"review_date\" TIMESTAMP, '\n",
    "    '\"reviewer_name\" STRING, '\n",
    "    '\"review_comments\" STRING, '\n",
    "    '\"sentiment\" STRING, '\n",
    "    '\"created_at\" TIMESTAMP, '\n",
    "    '\"updated_at\" TIMESTAMP)'\n",
    ")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "4220ad31-7d44-41e4-a880-58b2fbde4528",
   "metadata": {},
   "outputs": [],
   "source": [
    "import math\n",
    "\n",
    "# Function to write DataFrame in batches\n",
    "def write_pandas_in_batches(conn, chunk, table_name, batch_size=100000):  \n",
    "    num_batches = math.ceil(len(chunk) / batch_size)  # Calculate number of batches\n",
    "    \n",
    "    for i in range(num_batches):\n",
    "        start_idx = i * batch_size\n",
    "        end_idx = start_idx + batch_size\n",
    "        batch_df = chunk.iloc[start_idx:end_idx]  # Slice the DataFrame\n",
    "        \n",
    "        print(f\"Writing batch {i+1}/{num_batches} with {len(batch_df)} rows...\")\n",
    "        write_pandas(conn, batch_df, table_name=table_name)\n",
    "        \n",
    "    print(\"All batches written successfully!\")\n",
    "\n",
    "# Example usage\n",
    "write_pandas_in_batches(conn, chunk, table_name=\"airbnb_tx_table1\", batch_size=500000)  # Adjust batch size as needed\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "335c1891-4ba2-446c-b266-25f18b4a92f5",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.8"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
```




## Python and Virtualenv setup, and dbt installation - Windows
---
I am currently working with python 3.11
https://www.python.org/downloads/release/python-3113/
### Virtuaenv Setup
---
```sql
cd Desktop
mkdir course
cd course

virtualenv venv
venv\Scripts\activate
```
## dbt setup
---
initialize the dbt profile on windows
```sql
mkdir %userprofile%\.dbt
```
## Create a dbt project
```sql
dbt init cap
```
## Models
---
codes used for transformation
---
SRC Tx_table

``
models/src/src_listings.sql:
``
```sql
SELECT 
* 
FROM 
        AIRBNB.RAW."airbnb_tx_table"
```
Dim listings

``
models/dim/dim_listings_cleansed.sql
``
```sql
{{ 
  config(
    materialized = 'view'
  ) 
}}
WITH src_airbnb AS (
    SELECT * FROM {{ ref("src_airbnb") }}
),
deduplicated_listing AS (
    SELECT 
        "listing_id", 
        "name" AS "listing_name", 
        "room_type", 
        CASE 
            WHEN "minimum_nights" = 0 THEN 1 
            ELSE "minimum_nights" 
        END AS "minimum_nights", 
        "host_id", 
        "price", 
        cast("created_at" AS DATE) AS "created_at",
        cast("updated_at" AS DATE) AS "updated_at",
        ROW_NUMBER() OVER (PARTITION BY "listing_id" ORDER BY "updated_at" DESC) AS row_num
    FROM src_airbnb
),
Unique_listing AS (
    SELECT 
        "listing_id", 
        "listing_name", 
        "room_type", 
        "minimum_nights", 
        "host_id", 
        "price", 
        cast("created_at" AS DATE) AS "created_at",
        cast("updated_at" AS DATE) AS "updated_at"
    FROM deduplicated_listing
    WHERE row_num = 1
)
SELECT 
    MD5(CONCAT("listing_id", "updated_at")) AS "listing_sk",  -- Surrogate key
    "listing_id", 
    "listing_name", 
    "room_type", 
    "minimum_nights", 
    "host_id", 
    "price", 
        cast("created_at" AS DATE) AS "created_at",
        cast("updated_at" AS DATE) AS "updated_at"
FROM Unique_listing

```
Dim hosts

``
models/dim/dim_hosts_cleansed.sq
``
```sql
{{ 
  config(
    materialized = 'view'
  ) 
}}

-- models/dim_host.sql
WITH src_airbnb AS (
    SELECT * FROM {{ ref("src_airbnb") }}
),
deduplicated_hosts AS (
    SELECT 
        "host_id",
        NVL("host_name", 'Anonymous') AS "host_name",
        "is_superhost",
        cast("created_at" AS DATE) AS "created_at",
        cast("updated_at" AS DATE) AS "updated_at",
        ROW_NUMBER() OVER (PARTITION BY "host_id" ORDER BY CAST("updated_at" as date) DESC) AS row_num
    FROM src_airbnb
),
unique_hosts AS (
    SELECT 
        "host_id", 
        "host_name",
        "is_superhost",
        cast("created_at" AS DATE) AS "created_at",
        cast("updated_at" AS DATE) AS "updated_at"
    FROM deduplicated_hosts
    WHERE row_num = 1
)
SELECT 
    MD5(CONCAT("host_id", "updated_at")) AS "host_sk",  -- Surrogate key
    "host_id",
    "host_name",
    "is_superhost",
    cast("created_at" AS DATE) AS "created_at",
    cast("updated_at" AS DATE) AS "updated_at"
FROM unique_hosts

```
## Incremental model
---
``
fct/fct_reviews.sql
``
``` sql
{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
  )
}}

WITH src_reviews AS (
  SELECT * FROM {{ ref('src_airbnb') }}
),

max_review_date AS (
  {% if is_incremental() %}
  SELECT cast (MAX("review_date")AS DATE) AS max_date FROM {{ ref('src_airbnb') }}
  {% else %}
  SELECT NULL AS max_date
  {% endif %}
)

SELECT 
    MD5(CONCAT("listing_id", "updated_at")) AS "listing_sk",  -- Surrogate key
    "listing_id",
    CAST("review_date" AS DATE) AS "review_date",
    "reviewer_name",
    "review_comments",
    "sentiment"
FROM src_reviews
JOIN max_review_date ON 1=1
WHERE "review_comments" IS NOT NULL
{% if is_incremental() %}
  AND "review_date" > max_review_date.max_date
{% endif %}

```
## Get every review for listing 3176:

``` sql
SELECT * FROM "AIRBNB"."DEV"."FCT_REVIEWS" WHERE listing_id=3176;
```
## Add a new record to the table:
``` sql
INSERT INTO AIRBNB.RAW.RAW_REVIEWS 
VALUES (3493, CURRENT_TIMESTAMP(), 'Moses', 'crazy!', 'positive');
VALUES (3493, '2022-12-13', 'Moses', 'damn!', 'positive');
```







Dim listings with host
---
``
dim/dim_listings_w_hosts.sq
``
``` sql
WITH 
l AS ( 
    SELECT 
        * 
    FROM 
        {{ ref('dim_listings_cleansed') }} 
), 
h AS ( 
    SELECT *  
    FROM {{ ref('dim_hosts_cleansed') }} 
) 

SELECT
    MD5(CONCAT(l."listing_id", l."updated_at")) AS "listing_sk",  -- Surrogate key
    l."listing_id", 
    l."listing_name", 
    l."room_type", 
    CAST(l."minimum_nights" AS INT) AS "minimum_nights",
    CAST(l."price"AS INT) AS "price",
    l."host_id", 
    MD5(CONCAT(h."host_id", h."updated_at")) AS "host_sk", 
    h."host_name", 
    h."is_superhost" AS host_is_superhost, 
    CAST(l."created_at" AS DATE) AS "created_at",
    GREATEST(l."updated_at", h."updated_at") AS updated_at 
FROM l 
LEFT JOIN h ON h."host_id" = l."host_id"

```
## Dropping the views after ephemeral materialization
---
``
Drop VIEW AIRBNB.DEV.SRC_airbnb_tx;
``
## Contents of models/sources.yml
``` sql
version: 2

sources:
  - name: airbnb
    schema: raw
    tables:
      - name: airbnb_data
        identifier: airbnb_tx_table

```

## Seeds
---
## Full Moon Dates CSV
---
Download the CSV from from thIS GITHUB location:

``
curl -o seeds/festive_holidays_past_10_years.csv https://raw.githubusercontent.com/Moses-Otu/my-dbt-project/main/festive_holidays_past_10_years.csv
``

Then place it to the ``seeds`` folder

## Contents of models/mart/full_moon_reviews.sql

``` sql
{{ config(
  materialized = 'table'
) }}

WITH fct_reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),
holiday_dates AS (
    SELECT DISTINCT Date AS holiday_date FROM {{ ref('festive_holidays_past_10_years') }}
)

SELECT 
    r."listing_id",
    r."review_date",
    r."reviewer_name",
    r."review_comments",
    r."sentiment",
    CASE 
        WHEN d.holiday_date IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS is_holiday_review
FROM fct_reviews r
LEFT JOIN holiday_dates d 
    ON r."review_date" = d.holiday_date

```
# Snapshots (SCD 2 )
---
SCD2 for listing
---
``` sql
{% snapshot scd_raw_listings %}

{{
   config(
       target_schema='DEV',
       unique_key='listing_id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}


SELECT 
    listing_id,hotel_name,hotel_url,room_type,minimum_nights,host_id,price,created_at,
    updated_at::TIMESTAMP_NTZ AS updated_at  -- Ensure it's the correct datatype
FROM {{ ref("src_listings") }}

{% endsnapshot %}
-- This snapshot will track changes to the src_listings table based on the updated_at column.
-- It will create a new record in the snapshot table whenever there is a change in the listing_id or updated_at column.
```
## Updating the table
``` sql
UPDATE AIRBNB.RAW.RAW_LISTINGS SET price = 500,
updated_at = current_timestamp() where ID=1

select * from AIRBNB.DEV.SCD_RAW_HOSTS where HOST_ID = 1
```

## SCD 2 for owners
``` sql
{% snapshot scd_raw_hosts %}

{{
   config(
       target_schema='DEV',
       unique_key='host_id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}


SELECT 
    host_id,owner_name,is_superhost,created_at,
    updated_at::TIMESTAMP_NTZ AS updated_at  -- Ensure it's the correct datatype
FROM {{ ref("src_hosts") }}

{% endsnapshot %}
-- This snapshot will track changes to the src_listings table based on the updated_at column.
-- It will create a new record in the snapshot table whenever there is a change in the listing_id or updated_at column.
```
## updating the table
``` sql
UPDATE AIRBNB.RAW.RAW_HOSTS SET NAME = 'MOSES OTU',
updated_at = current_timestamp() where ID=1
```
# Test

## General Test

## The contents of models/schema.yml:
``` sql
version: 2

models:
  - name: dim_listings_cleansed
    columns:

     - name: listing_id
       tests:
         - unique
         - not_null
         - relationships:
             to: ref('dim_listings_w_owners')
             field: listing_id

models:
  - name: dim_owners_cleansed
    columns:
      - name: host_id
        tests:
          - unique
          - not_null

```
# Reporter role and Preset user in Snowflake

``` sql
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS REPORTER;
CREATE USER IF NOT EXISTS PRESET
 PASSWORD='presetPassword123'
 LOGIN_NAME='preset'
 MUST_CHANGE_PASSWORD=FALSE
 DEFAULT_WAREHOUSE='COMPUTE_WH'
 DEFAULT_ROLE=REPORTER
 DEFAULT_NAMESPACE='AIRBNB.DEV'
 COMMENT='Preset user for creating reports';

GRANT ROLE REPORTER TO USER PRESET;
GRANT ROLE REPORTER TO ROLE ACCOUNTADMIN;
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE REPORTER;
GRANT USAGE ON DATABASE AIRBNB TO ROLE REPORTER;
GRANT USAGE ON SCHEMA AIRBNB.DEV TO ROLE REPORTER;

-- We don't want to grant select rights here; we'll do this through hooks:
-- GRANT SELECT ON ALL TABLES IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON FUTURE VIEWS IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;

```

# Creating a Dashboard in Preset
---
Getting the Snowflake credentials up to the screen:

-- Windows (cmd): type %USERPROFILE%\.dbt\profiles.yml

## Exposure

``
The contents of models/dashboard.yml:
``
``` sql
version: 2

exposures:
  - name: executive_dashboard
    label: Executive Dashboard
    type: dashboard
    maturity: low
    url: https://9471b49c.us2a.app.preset.io/superset/dashboard/8/?native_filters_key=3xMmAdJpXGZRWtI1rqIgtGxd-lOft_OsTAnY_Uv-_C8vnHLchKgbR2axULzre8WD
    description: Executive Dashboard about Hostel listings and hosts
      

    depends_on:
      - ref('dim_listings_w_owners')
      - ref('holiday_reviews')

    owner:
      name: Moses Otu
      email: Jada@gmail.com
```
## Post-hook
``
Add this to your dbt_project.yml:
``
``` SQL
+post-hook:
      - "GRANT SELECT ON {{ this }} TO ROLE REPORTER"
```

# Dagster
--
We will create a virtualenv and install dbt and dagster. 

``
virtualenv venv -p python3.11
pip install -r requirements.txt
``
# I will recommend reading throught the dagster -dbt documentation to continue model orchestration

``` sql
https://docs.dagster.io/integrations/libraries/dbt/using-dbt-with-dagster/load-dbt-models
```
## Streaming data code

``` sql
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, current_date  # ✅ Import current_date
import random
from datetime import datetime

# Constants
BATCH_SIZE = 2  # Generate only 2 reviews per day
LISTINGS_COUNT = 1000
FIRST_NAMES = ["Alice", "Bob", "Charlie", "David"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown"]
REVIEW_COMMENTS = ["Great stay!", "Loved it!", "Not so great.", "Would visit again."]
SENTIMENTS = ["positive", "negative", "neutral"]

def generate_daily_reviews():
    """Generate 2 random reviews for today's date."""
    today = datetime.now().strftime('%Y-%m-%d')  # ✅ Ensure date format matches Snowflake

    rows = [
        (
            random.randint(1, LISTINGS_COUNT),
            today,  # ✅ Store date as a string in 'YYYY-MM-DD' format
            f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            random.choice(REVIEW_COMMENTS),
            random.choice(SENTIMENTS)
        )
        for _ in range(BATCH_SIZE)
    ]
    
    return rows

def main(session: snowpark.Session): 
    """Runs in a Snowflake Notebook and inserts directly into AIRBNB.RAW.RAW_REVIEWS."""
    
    # Generate today's review data
    reviews = generate_daily_reviews()
    
    # Convert to a Snowpark DataFrame
    df = session.create_dataframe(reviews, schema=["listing_id", "date", "reviewer_name", "comments", "sentiment"])
    
    # Append data directly to AIRBNB.RAW.RAW_REVIEWS
    df.write.mode("append").save_as_table("AIRBNB.RAW.RAW_REVIEWS")
    
    # ✅ Query only today's inserted data
    return session.table("AIRBNB.RAW.RAW_REVIEWS").filter(col("date") == current_date())
```
## Task automation and monitoring

```sql
--- CREATE AUTOMATION TASK TO RUN EVERYDAY AT MIDNIGHT

CREATE OR REPLACE TASK daily_append_reviews
  WAREHOUSE = COMPUTE_WH 
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Runs every day at midnight UTC
AS
  CALL append_reviews_daily();

-- Enable Task 
ALTER TASK daily_append_reviews RESUME;

-- SHOW TASK
SHOW TASKS LIKE 'daily_append_reviews';

-- SHOW AUTOMATION HISTORY
SELECT * FROM TABLE(information_schema.task_history())
  WHERE name = 'DAILY_APPEND_REVIEWS'
  ORDER BY completed_time DESC;

```
