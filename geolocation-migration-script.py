import psycopg2
import pandas as pd
from getpass import getpass
import argparse
import sys
import urllib.parse
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

def connect_to_db(connection_uri=None, db_info=None):
    """
    Connect to PostgreSQL database using either URI or individual parameters
    """
    try:
        if connection_uri:
            conn = psycopg2.connect(connection_uri)
        else:
            conn = psycopg2.connect(
                host=db_info["host"],
                database=db_info["database"],
                user=db_info["user"],
                password=db_info["password"],
                port=db_info["port"]
            )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def fetch_distinct_locations(conn, fact_table, province_col=None, city_col=None, district_col=None, 
                           subdistrict_col=None, province_value=None, city_value=None, 
                           district_value=None, subdistrict_value=None):
    """
    Fetch distinct location combinations from the fact table based on hierarchy
    Supports various combinations of columns and static values
    """
    columns = []
    select_clauses = []
    where_clauses = []
    
    # First determine which columns are available from the fact table
    available_columns = []
    for col_name, col_var in [
        ("province", province_col), 
        ("city", city_col), 
        ("district", district_col), 
        ("sub_district", subdistrict_col)
    ]:
        if col_var:
            columns.append(col_name)
            select_clauses.append(f"{col_var} as {col_name}")
            where_clauses.append(f"{col_var} IS NOT NULL")
            available_columns.append(col_name)
    
    # If no columns are specified, we need to verify we have at least one static value
    if not select_clauses:
        if not any([province_value, city_value, district_value, subdistrict_value]):
            print("Error: You must provide at least one column from the fact table or one static value")
            sys.exit(1)
        
        # Create a dummy SELECT to get at least one row
        select_sql = f"""
        SELECT 1
        FROM {fact_table}
        LIMIT 1
        """
    else:
        # Construct SQL query for columns
        select_sql = f"""
        SELECT DISTINCT {', '.join(select_clauses)}
        FROM {fact_table}
        """
        
        if where_clauses:
            select_sql += f" WHERE {' AND '.join(where_clauses)}"
    
    try:
        df = pd.read_sql(select_sql, conn)
        
        # If we have no columns, but used the dummy select, convert to empty DataFrame with proper columns
        if not select_clauses:
            df = pd.DataFrame(columns=["province", "city", "district", "sub_district"])
            # Add a single row that will be populated with static values
            df = pd.concat([df, pd.DataFrame([{
                "province": None, 
                "city": None, 
                "district": None, 
                "sub_district": None
            }])], ignore_index=True)
        
        # Add static values if provided and column doesn't exist
        if province_value and "province" not in available_columns:
            df["province"] = province_value
            columns.append("province")
        
        if city_value and "city" not in available_columns:
            df["city"] = city_value
            columns.append("city")
            
        if district_value and "district" not in available_columns:
            df["district"] = district_value
            columns.append("district")
            
        if subdistrict_value and "sub_district" not in available_columns:
            df["sub_district"] = subdistrict_value
            columns.append("sub_district")
            
        return df, columns
    except Exception as e:
        print(f"Error fetching distinct locations: {e}")
        print(f"SQL query: {select_sql}")
        sys.exit(1)

def lookup_location_ids(locations_df, columns):
    """
    Lookup location IDs from the geolocation master table
    Dynamic column selection based on provided hierarchy levels
    """
    neon_db_conn = connect_to_db(connection_uri=os.environ.get("MASTER_DB_URI"))
    results = []
    
    # Map user columns to database columns
    column_mapping = {
        'province': 'provinsi',
        'city': 'kota_kabupaten',
        'district': 'kecamatan',
        'sub_district': 'kelurahan_desa'
    }
    
    # Dynamically build select columns
    select_columns = ['objectid']  # Always include objectid
    for col in columns:
        if col in column_mapping:
            select_columns.append(column_mapping[col])
    
    # Create lookup_sql based on available columns
    for _, row in locations_df.iterrows():
        conditions = []
        for col in columns:
            if col in row and pd.notna(row[col]):
                # Escape single quotes in the data
                value = str(row[col]).replace("'", "''")
                db_col = column_mapping[col]
                conditions.append(f"LOWER({db_col}) = LOWER('{value}')")
        
        if not conditions:
            continue
            
        lookup_sql = f"""
        SELECT {', '.join(select_columns)}
        FROM indonesia_boundaries
        WHERE {' AND '.join(conditions)}
        LIMIT 1
        """
        
        try:
            cursor = neon_db_conn.cursor()
            cursor.execute(lookup_sql)
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                # Create a mapping of column names to values
                result_dict = {'location_id': result[0]}  # First column is always objectid
                for i, col in enumerate(columns, 1):
                    if i < len(result):  # Make sure we don't exceed the result tuple
                        result_dict[col] = result[i]
                    else:
                        result_dict[col] = row.get(col)  # Fallback to input value
                
                results.append(result_dict)
            else:
                print(f"Warning: No location found for {row.to_dict()}")
        except Exception as e:
            print(f"Error looking up location ID: {e}")
            print(f"SQL query: {lookup_sql}")
    
    neon_db_conn.close()
    return results, select_columns

def create_geo_ref_table(conn, args):
    """
    Create the geo_ref table if it doesn't exist with column names matching user input
    Also includes columns for static values if only values are provided
    """
    # Build column definitions based on user input and static values
    columns = ['location_id VARCHAR(255) NOT NULL']
    
    # Add columns for either column names or static values
    if args.province_col or args.province:
        column_name = args.province_col if args.province_col else "province"
        columns.append(f'"{column_name}" VARCHAR(255)')
    
    if args.city_col or args.city:
        column_name = args.city_col if args.city_col else "city"
        columns.append(f'"{column_name}" VARCHAR(255)')
    
    if args.district_col or args.district:
        column_name = args.district_col if args.district_col else "district"
        columns.append(f'"{column_name}" VARCHAR(255)')
    
    if args.subdistrict_col or args.subdistrict:
        column_name = args.subdistrict_col if args.subdistrict_col else "sub_district"
        columns.append(f'"{column_name}" VARCHAR(255)')
    
    columns.append('created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS geo_ref (
        {', '.join(columns)}
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        print("Geo reference table created or already exists")
    except Exception as e:
        print(f"Error creating geo_ref table: {e}")
        print(f"SQL query: {create_table_sql}")
        sys.exit(1)

def insert_into_geo_ref(conn, locations, select_columns, args):
    """
    Insert location data into the geo_ref table using column names from user input or default names for static values
    """
    if not locations:
        print("No locations to insert")
        return
    
    # Build insert columns list - start with location_id
    insert_columns = ['location_id']

    # Add columns based on either provided column names or static values
    if args.province_col or args.province:
        col_name = args.province_col if args.province_col else "province"
        insert_columns.append(f'"{col_name}"')
    
    if args.city_col or args.city:
        col_name = args.city_col if args.city_col else "city"
        insert_columns.append(f'"{col_name}"')
    
    if args.district_col or args.district:
        col_name = args.district_col if args.district_col else "district"
        insert_columns.append(f'"{col_name}"')
    
    if args.subdistrict_col or args.subdistrict:
        col_name = args.subdistrict_col if args.subdistrict_col else "sub_district"
        insert_columns.append(f'"{col_name}"')

    placeholders = ', '.join(['%s'] * len(insert_columns))
    insert_sql = f"""
    INSERT INTO geo_ref ({', '.join(insert_columns)})
    VALUES ({placeholders})
    """
    
    try:
        cursor = conn.cursor()
        for loc in locations:
            values = [loc['location_id']]  # Start with location_id
            
            # Add values for each column
            if args.province_col or args.province:
                values.append(loc.get('province') if args.province_col else args.province)
            
            if args.city_col or args.city:
                values.append(loc.get('city') if args.city_col else args.city)
            
            if args.district_col or args.district:
                values.append(loc.get('district') if args.district_col else args.district)
            
            if args.subdistrict_col or args.subdistrict:
                values.append(loc.get('sub_district') if args.subdistrict_col else args.subdistrict)
            
            cursor.execute(insert_sql, values)
        
        conn.commit()
        cursor.close()
        print(f"Successfully inserted {len(locations)} locations into geo_ref table")
    except Exception as e:
        print(f"Error inserting into geo_ref table: {e}")
        print(f"Failed SQL: {insert_sql}")
        print(f"Values: {values}")
        conn.rollback()
        sys.exit(1)

def validate_hierarchy(args):
    """
    Validate the location hierarchy based on provided columns and values
    """
    # Maps to track what we have for each level in the hierarchy
    has_column = {
        'province': bool(args.province_col),
        'city': bool(args.city_col),
        'district': bool(args.district_col),
        'subdistrict': bool(args.subdistrict_col)
    }
    
    has_value = {
        'province': bool(args.province),
        'city': bool(args.city),
        'district': bool(args.district),
        'subdistrict': bool(args.subdistrict)
    }
    
    # Check if we have at least one input (column or value)
    if not any(list(has_column.values()) + list(has_value.values())):
        print("Error: You must provide at least one location column or value")
        return False
        
    # If we have province column only, that's valid
    if has_column['province'] and not any([
        has_column['city'], has_column['district'], has_column['subdistrict'],
        has_value['province'], has_value['city'], has_value['district'], has_value['subdistrict']
    ]):
        return True
        
    # If we have province value and city column only, that's valid
    if has_value['province'] and has_column['city'] and not any([
        has_column['province'], has_column['district'], has_column['subdistrict'],
        has_value['city'], has_value['district'], has_value['subdistrict']
    ]):
        return True
        
    # For each level in hierarchy, we must have either a column or a value for all levels above
    hierarchy = ['province', 'city', 'district', 'subdistrict']
    
    for i, level in enumerate(hierarchy):
        # Skip the first level (province)
        if i == 0:
            continue
            
        # If current level has a column
        if has_column[level]:
            # Check that all levels above have either a column or value
            for upper_level in hierarchy[:i]:
                if not (has_column[upper_level] or has_value[upper_level]):
                    print(f"Error: {level} column provided but missing {upper_level} column or value")
                    return False
    
    # All checks passed
    return True

def main():
    parser = argparse.ArgumentParser(description='Create geolocation reference table migration')
    
    # Connection group - either URI or individual parameters
    connection_group = parser.add_mutually_exclusive_group(required=True)
    connection_group.add_argument('--connection-uri', help='Database connection URI (postgres://user:password@host:port/database)')
    connection_group.add_argument('--host', help='Database host')
    
    # Other connection parameters (only used if --host is provided)
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', help='Database name')
    parser.add_argument('--user', help='Database user')
    
    # Required parameters
    parser.add_argument('--fact-table', required=True, help='Fact table name')
    
    # Location columns
    parser.add_argument('--province-col', help='Province column name in fact table')
    parser.add_argument('--city-col', help='City column name in fact table')
    parser.add_argument('--district-col', help='District column name in fact table')
    parser.add_argument('--subdistrict-col', help='Sub-district column name in fact table')
    
    # Static location values
    parser.add_argument('--province', help='Static province value (use when province column not available)')
    parser.add_argument('--city', help='Static city value (use when city column not available)')
    parser.add_argument('--district', help='Static district value (use when district column not available)')
    parser.add_argument('--subdistrict', help='Static sub-district value (use when sub-district column not available)')
    
    args = parser.parse_args()
    
    # Validate hierarchy
    if not validate_hierarchy(args):
        sys.exit(1)
    
    # Connect to database
    if args.connection_uri:
        conn = connect_to_db(connection_uri=args.connection_uri)
    else:
        # Validate required parameters when using individual connection details
        if not all([args.host, args.database, args.user]):
            print("Error: When not using --connection-uri, you must specify --host, --database, and --user")
            sys.exit(1)
            
        # Get password securely
        password = getpass("Enter database password: ")
        
        db_info = {
            "host": args.host,
            "port": args.port,
            "database": args.database,
            "user": args.user,
            "password": password
        }
        conn = connect_to_db(db_info=db_info)
    
    # Fetch distinct locations from fact table
    print(f"Fetching distinct locations from {args.fact_table}...")
    locations_df, columns = fetch_distinct_locations(
        conn, 
        args.fact_table, 
        args.province_col, 
        args.city_col, 
        args.district_col, 
        args.subdistrict_col,
        args.province,
        args.city,
        args.district,
        args.subdistrict
    )
    print(f"Found {len(locations_df)} distinct location combinations")
    
    # Lookup location IDs
    print("Looking up location IDs from geo_location_master...")
    locations, select_columns = lookup_location_ids(locations_df, columns)
    print(f"Found {len(locations)} matching location IDs")
    
    # Create geo_ref table with dynamic column names
    create_geo_ref_table(conn, args)
    
    # Insert data into geo_ref table
    insert_into_geo_ref(conn, locations, select_columns, args)
    
    conn.close()
    print("Migration completed successfully")

if __name__ == "__main__":
    main()