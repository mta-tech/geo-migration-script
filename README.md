# Geolocation Migration Script

A Python script for creating a geolocation reference table by mapping location data from a fact table to standardized location IDs from a master geolocation database (indonesia_boundaries).

## Features

- Maps location data from your fact table to standardized location IDs
- Supports flexible hierarchy levels (province, city, district, sub-district)
- Handles both column-based and static value mappings
- Creates a reference table with customizable column names
- Validates location hierarchy to ensure data integrity
- Supports both URI and individual parameter database connections

## Prerequisites

- Python 3.11 or higher
- PostgreSQL database
- Required Python packages (installed automatically via pyproject.toml):
  - pandas >= 2.2.3
  - psycopg2-binary >= 2.9.10

## Installation

1. Clone this repository:
```bash
git clone [repository-url]
cd geolocation-migration-script
```

2. Install dependencies:
```bash
pip install -e .
```

## Environment Configuration

The script uses environment variables for database configuration. Create a `.env` file in the root directory with the following structure:

```properties
MASTER_DB_URI=postgresql://<username>:<password>@<host>/<database>?sslmode=require
```

### Environment Variables Description

- `MASTER_DB_URI`: PostgreSQL connection string for the master geolocation database containing the following components:
  - `username`: Database user
  - `password`: Database password
  - `host`: Database host address
  - `database`: Database name
  - `sslmode`: SSL mode for connection (required for secure connections)

### Example
```properties
MASTER_DB_URI=postgresql://geo_owner:your_password@your-host.example.com/geo?sslmode=require
```

> **Note**: Never commit the actual `.env` file to version control. Make sure to add `.env` to your `.gitignore` file to prevent exposing sensitive information.

## Usage

The script can be run with various command-line arguments to specify the source data and connection details.

### Basic Usage Example

```bash
python geolocation-migration-script.py \
  --connection-uri "postgres://user:password@host:port/database" \
  --fact-table "your_fact_table" \
  --province-col "province_column" \
  --city-col "city_column" \
  --district-col "district_column"
```

### Available Arguments

#### Database Connection (Required, choose one option):
- `--connection-uri`: Full database connection URI (postgres://user:password@host:port/database)
  OR
- `--host`: Database host
- `--port`: Database port (default: 5432)
- `--database`: Database name
- `--user`: Database username

#### Required Arguments:
- `--fact-table`: Name of the source fact table containing location data

#### Location Column Arguments (Optional):
- `--province-col`: Column name for province in fact table
- `--city-col`: Column name for city in fact table
- `--district-col`: Column name for district in fact table
- `--subdistrict-col`: Column name for sub-district in fact table

#### Static Location Values (Optional):
- `--province`: Static province value when column not available
- `--city`: Static city value when column not available
- `--district`: Static district value when column not available
- `--subdistrict`: Static sub-district value when column not available

### Hierarchy Rules

The script enforces certain hierarchy rules:
1. At least one location identifier (column or static value) must be provided
2. When using multiple levels, all higher levels must be specified
   - Example: If using district, both province and city must be specified (either as columns or static values)
3. Province-only queries are allowed
4. Static province value with city column is allowed

### Output Table Structure

The script creates a table named `geo_ref_test` with the following structure:
- `location_id`: VARCHAR(255) NOT NULL - The standardized location ID
- Column names matching your input column names (all VARCHAR(255))
- `created_at`: TIMESTAMP - Record creation timestamp

### Examples

1. Using all column mappings:
```bash
python geolocation-migration-script.py \
  --host "localhost" \
  --database "yourdb" \
  --user "youruser" \
  --fact-table "sales_data" \
  --province-col "state_name" \
  --city-col "city_name" \
  --district-col "district_name" \
  --subdistrict-col "village_name"
```

2. Using static province with dynamic city and district:
```bash
python geolocation-migration-script.py \
  --connection-uri "postgres://user:password@host:port/database" \
  --fact-table "customer_data" \
  --province "JAWA BARAT" \
  --city-col "city" \
  --district-col "district"
```

3. Using province column only:
```bash
python geolocation-migration-script.py \
  --connection-uri "postgres://user:password@host:port/database" \
  --fact-table "regional_sales" \
  --province-col "province_name"
```

## Error Handling

The script includes various error checks:
- Database connection validation
- Hierarchy validation
- Column existence verification
- Data type validation
- Null value handling

If any errors occur, the script will provide detailed error messages and exit gracefully.

## Notes

- Column names in the output table will exactly match the input column names
- The script uses case-insensitive matching for location names
- All string fields are limited to VARCHAR(255)
- The script automatically handles SQL injection prevention
- Transactions are used to ensure data consistency