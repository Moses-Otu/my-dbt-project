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
  PASSWORD='dbtPassword123'
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
    "#%pip install snowflake-snowpark-python"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "91bb50ee-ae65-4898-962d-e14de2734996",
   "metadata": {},
   "outputs": [],
   "source": [
    "#!pip install \"snowflake-connector-python[pandas]\""
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
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import random\n",
    "from datetime import datetime, timedelta\n",
    "\n",
    "# Configuration\n",
    "TOTAL_ROWS = 5500000\n",
    "BATCH_SIZE = 50000\n",
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
   "id": "c5e4f7c4-a481-44e1-bc0c-bf362ec1a2cf",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Read the Parquet file into a DataFrame\n",
    "df = pd.read_csv('airbnb_fact_data.csv')"
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
    "    password='xxxxxxxxx',##use your original pasword for snowflakes\n",
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
    "    'CREATE TABLE \"airbnb_tx_table\" ('\n",
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
    "def write_pandas_in_batches(conn, df, table_name, batch_size=100000):  \n",
    "    num_batches = math.ceil(len(df) / batch_size)  # Calculate number of batches\n",
    "    \n",
    "    for i in range(num_batches):\n",
    "        start_idx = i * batch_size\n",
    "        end_idx = start_idx + batch_size\n",
    "        batch_df = df.iloc[start_idx:end_idx]  # Slice the DataFrame\n",
    "        \n",
    "        print(f\"Writing batch {i+1}/{num_batches} with {len(batch_df)} rows...\")\n",
    "        write_pandas(conn, batch_df, table_name=table_name)\n",
    "        \n",
    "    print(\"All batches written successfully!\")\n",
    "\n",
    "# Example usage\n",
    "write_pandas_in_batches(conn, df, table_name=\"airbnb_tx_table\", batch_size=500000)  # Adjust batch size as needed\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "0743dea5-559c-4425-81af-a5c89ed6e1d4",
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
    SELECT * FROM {{ ref('src_airbnb') }}
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
        "created_at", 
        "updated_at", 
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
        "created_at", 
        "updated_at"
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
    "created_at", 
    "updated_at"
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
    SELECT * FROM {{ ref('src_airbnb') }}
),
deduplicated_hosts AS (
    SELECT 
        "host_id",
        NVL("host_name", 'Anonymous') AS "host_name",
        "is_superhost",
        "created_at",
        "updated_at",
        ROW_NUMBER() OVER (PARTITION BY "host_id" ORDER BY "updated_at" DESC) AS row_num
    FROM src_airbnb
),
unique_hosts AS (
    SELECT 
        "host_id", 
        "host_name",
        "is_superhost",
        "created_at",
        "updated_at"
    FROM deduplicated_hosts
    WHERE row_num = 1
)
SELECT 
    MD5(CONCAT("host_id", "updated_at")) AS "host_sk",  -- Surrogate key
    "host_id",
    "host_name",
    "is_superhost",
    "created_at",
    "updated_at"
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
    on_schema_change = 'fail'
  ) 
}}

WITH src_airbnb AS (
    SELECT * FROM {{ ref('src_airbnb') }}
)

SELECT 
    "listing_id", 
    "review_date", 
    "reviewer_name", 
    "review_comments", 
    "sentiment"
FROM src_airbnb
WHERE "review_comments" IS NOT NULL

{% if is_incremental() %}
AND "review_date" > (SELECT MAX("review_date") FROM {{ this }})
{% endif %}
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
    MD5(CONCAT(l."listing_id", GREATEST(l."updated_at", h."updated_at"))) AS listing_sk,
    l."listing_id", 
    l."listing_name", 
    l."room_type", 
    l."minimum_nights", 
    l."price", 
    l."host_id", 
    MD5(CONCAT(l."host_id", GREATEST(l."updated_at", h."updated_at"))) AS host_sk,
    h."host_name", 
    h."is_superhost" as host_is_superhost, 
    l."created_at", 
    GREATEST(l."updated_at", h."updated_at") as updated_at 
FROM l 
LEFT JOIN h ON (h."host_id" = l."host_id")
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
    CAST(r."review_date" AS DATE) AS review_date,
    r."reviewer_name",
    r."review_comments",
    r."sentiment",
    CASE 
        WHEN d.holiday_date IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS is_holiday_review
FROM fct_reviews r
LEFT JOIN holiday_dates d 
    ON CAST(r."review_date" AS DATE) = d.holiday_date

```

