from datetime import datetime, time, timezone,timedelta
import pytz
# from config import ACCESS_TOKEN, INSTANCE_URL
import requests
import urllib.parse
from services.salesforce_client import execute_soql_query
import logging
import json
from requests.exceptions import HTTPError
from queries.insert_query import SalesforceUpsertFunctions
from database.connection_manager import get_connection
inserter = SalesforceUpsertFunctions()
class HelperFunc:
    def __init__(self, get_connection):
        self.get_connection = get_connection
    
    def get_last_sync_time(self, table_name):
        """Get the last synchronization timestamp from your database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(last_sync_time) 
                FROM sync_log 
                WHERE table_name = ?
            """, (table_name,))
            result = cursor.fetchone()
            return result[0] if result[0] else None

    def update_sync_log(self, table_name, sync_time):
        """Update the last sync time using SQL Server MERGE statement"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                MERGE sync_log AS target
                USING (SELECT ? AS table_name, ? AS last_sync_time) AS source
                ON (target.table_name = source.table_name)
                WHEN MATCHED THEN
                    UPDATE SET last_sync_time = source.last_sync_time, created_at = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (table_name, last_sync_time, created_at)
                    VALUES (source.table_name, source.last_sync_time, GETDATE());
            """, (table_name, sync_time))
            conn.commit()
            print(f"Upserted sync log for {table_name}")


    # def tooling_query(self,soql,company_id):
    #     soql_clean = soql.strip()
    #     token_data = self.get_salesforce_token_from_db(company_id)
    #     INSTANCE_URL = token_data["Metadata"].get("instance_url", "")
    #     ACCESS_TOKEN = token_data['AccessToken']
    #     url = f"{INSTANCE_URL}/services/data/v60.0/tooling/query?q={urllib.parse.quote(soql_clean)}"
        
    #     resp = requests.get(url, headers={'Authorization': f'Bearer {ACCESS_TOKEN}'})
    #     resp.raise_for_status()
    #     return resp.json().get('records', [])
    def tooling_query(self,soql,company_id):
        token_data = self.get_salesforce_token_from_db(company_id)
        if not token_data:
            raise Exception("Salesforce token not found")

        access_token = token_data["AccessToken"]
        refresh_token = token_data.get("RefreshToken")
        instance_url = token_data["Metadata"].get("instance_url", "")
        token_id = token_data["Id"]
        user_id = token_data["UserId"]

        def execute(token, inst_url):
            url = f"{inst_url}/services/data/v60.0/tooling/query?q={urllib.parse.quote(soql.strip())}"
            resp = requests.get(url, headers={'Authorization': f'Bearer {token}'})
            resp.raise_for_status()
            return resp.json().get("records", [])

        try:
            return execute(access_token, instance_url)

        except HTTPError as err:
            if err.response and err.response.status_code == 401:
                from routes.oauth_routes import refresh_access_token

                refreshed = refresh_access_token(refresh_token)
                if not refreshed or "access_token" not in refreshed:
                    raise Exception("Token refresh failed or invalid response")

                expires_in = int(refreshed.get("expires_in", 14400))
                new_metadata = json.dumps({
                    "instance_url": refreshed.get("instance_url", instance_url),
                    "token_type": refreshed.get("token_type", ""),
                    "expires_in_seconds": refreshed.get("expires_in", ""),
                    "token_id": refreshed.get("id", "")
                })

                updated_data = {
                    "Id": token_id,
                    "ProviderName": token_data.get("ProviderName", 3),
                    "ServiceType": token_data.get("ServiceType", 3),
                    "AccessToken": refreshed["access_token"],
                    "RefreshToken": refreshed.get("refresh_token", refresh_token),
                    "ExpireAt": datetime.utcnow() + timedelta(seconds=expires_in),
                    "UserId": user_id,
                    "CreatedBy": user_id,
                    "CreatedAt": datetime.utcnow(),
                    "Version": token_data.get("Version", 1),
                    "ModifiedBy": user_id,
                    "ModifiedAt": datetime.utcnow(),
                    "IsDeleted": 0,
                    "CompanyId": company_id,
                    "Metadata": new_metadata,
                    "Status": 0
                }
                inserter.upsert_access_token(updated_data)
                return execute(refreshed["access_token"], refreshed.get("instance_url", instance_url))
            raise


    def sf_custom_fields_for_object(self,object_name,company_id, last_sync=None):
        """
        Returns a list of custom fields for a Salesforce object with incremental sync support.
        
        Args:
            object_name: Salesforce object name (e.g., 'Account', 'Contact')
            last_sync: datetime object of last sync time, or None for full sync
        """
        print(f"Processing custom fields for {object_name}")
        print(f"Last sync time: {last_sync}")
        
        # 1. Get metadata definitions for all fields on the object
        fd = self.tooling_query(f"""
            SELECT QualifiedApiName, Label, DataType, Description
            FROM FieldDefinition
            WHERE EntityDefinition.QualifiedApiName = '{object_name}'
        """,company_id)
        fd_map = {r['QualifiedApiName']: r for r in fd}

        # 2. Get flags for each field: is it unique or deprecated?
        ep = self.tooling_query(f"""
            SELECT FieldDefinition.QualifiedApiName, IsUnique, IsDeprecatedAndHidden
            FROM EntityParticle
            WHERE EntityDefinition.QualifiedApiName = '{object_name}'
        """,company_id)
        ep_map = {r['FieldDefinition']['QualifiedApiName']: r for r in ep}

        # 3. List only custom fields created by users (names end with '__c')
        cf = self.tooling_query(f"""
            SELECT Id, DeveloperName, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById
            FROM CustomField
            WHERE TableEnumOrId = '{object_name}'
        """,company_id)
        cf_map = {r['DeveloperName']: r for r in cf}

        # 4. Use REST API describe call to get additional properties
        
        
        token_data = self.get_salesforce_token_from_db(company_id)
        ACCESS_TOKEN = token_data['AccessToken']
        INSTANCE_URL = token_data["Metadata"].get("instance_url", "")
        resp = requests.get(
            f"{INSTANCE_URL}/services/data/v60.0/sobjects/{object_name}/describe",
            headers={'Authorization': f'Bearer {ACCESS_TOKEN}'}
        )
        resp.raise_for_status()
        dmap = {f['name']: f for f in resp.json().get('fields', [])}

        # 5. Get actual field values with incremental sync logic
        custom_field_names = [f"{dev_name}__c" for dev_name in cf_map.keys()]
        print(f"Custom field names for {object_name}: {custom_field_names}")
        
        field_values_map = {}
        if custom_field_names:
            # Build SOQL query based on sync type
            fields_str = ", ".join(custom_field_names)
            
            if last_sync:
                # INCREMENTAL SYNC - only get records modified since last sync
                print(f"Performing incremental sync for {object_name}")
                
                if not isinstance(last_sync, datetime):
                    raise TypeError(f"Expected datetime.datetime, got {type(last_sync)}")
                
                # Format for SOQL: ISO8601 with milliseconds and UTC
                formatted = last_sync.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                soql_query = f"""
                    SELECT Id, {fields_str} 
                    FROM {object_name} 
                    WHERE LastModifiedDate > {formatted}
                    ORDER BY LastModifiedDate ASC
                """
                print(f"Incremental SOQL query for {object_name}: {soql_query}")
            else:
                # FULL SYNC - get all records (first time)
                print(f"Performing full sync for {object_name}")
                soql_query = f"""
                    SELECT Id, {fields_str} 
                    FROM {object_name} 
                    ORDER BY LastModifiedDate ASC
                """
                print(f"Full SOQL query for {object_name}: {soql_query}")
            
            try:
                # Execute the SOQL query to get field values
                records = execute_soql_query(ACCESS_TOKEN, INSTANCE_URL, soql_query)
                print(f"Found {len(records)} records for {object_name}")
                
                # Process each record to collect field values
                for record in records:
                    record_id = record.get('Id')
                    for field_name in custom_field_names:
                        if field_name not in field_values_map:
                            field_values_map[field_name] = []
                        
                        field_value = record.get(field_name)
                        if field_value is not None:  # Only add non-null values
                            field_values_map[field_name].append({
                                'record_id': record_id,
                                'value': field_value
                            })
            except Exception as e:
                print(f"Error fetching field values for {object_name}: {str(e)}")
                # Continue with metadata even if values can't be fetched

        # 6. Only return data if there are actual field values to process
        # For incremental sync, if no records were modified, don't return metadata
        if last_sync and not field_values_map:
            print(f"No modified records found for {object_name} since {last_sync}")
            return []  # Return empty list - no data to upsert
        
        # If we have field values OR it's a full sync, combine metadata with values
        result = []
        for dev_name, cf_rec in cf_map.items():
            api_name = f"{dev_name}__c"
            fd_rec = fd_map.get(api_name, {})
            d = dmap.get(api_name, {})
            ep_rec = ep_map.get(api_name, {})

            # Check if the field is marked deprecated/hidden
            is_active = not ep_rec.get('IsDeprecatedAndHidden', False)
            
            # Get field values
            field_values = field_values_map.get(api_name, [])
            
            # For incremental sync, only include fields that have actual values
            # For full sync, include all custom fields (even if no current values)
            if not last_sync or field_values:
                result.append({
                    "Object": object_name,
                    "CustomFieldId": cf_rec.get('Id'),
                    "Name": api_name,
                    "Label": d.get('label'),
                    "Description": fd_rec.get('Description'),
                    "HelpText": d.get('inlineHelpText'),
                    "FieldType": d.get('type'),
                    "DataType": fd_rec.get('DataType'),
                    "IsUnique": ep_rec.get('IsUnique', False),
                    "IsActive": is_active,
                    "CreatedBy": cf_rec.get('CreatedById'),
                    "UpdatedBy": cf_rec.get('LastModifiedById'),
                    "CreatedAt": cf_rec.get('CreatedDate'),
                    "UpdatedAt": cf_rec.get('LastModifiedDate'),
                    "FieldValues": field_values,  # All values with record IDs
                })

        print(f"Returning {len(result)} custom fields for {object_name}")
        return result
    
    def get_salesforce_token_from_db(self, company_id):
        try:
            
            with get_connection() as conn:
                cursor = conn.cursor()

                logging.info("Getting Salesforce token for Company Id: %s", company_id)

                query = """
                    SELECT TOP 1 
                    Id,
                    ProviderName,
                    ServiceType,
                    AccessToken,
                    RefreshToken,
                    CAST(ExpireAt AS DATETIME) AS ExpireAt,
                    UserId,
                    CreatedBy,
                    CAST(CreatedAt AS DATETIME) AS CreatedAt,
                    Version,
                    ModifiedBy,
                    CAST(ModifiedAt AS DATETIME) AS ModifiedAt,
                    IsDeleted,
                    CompanyId,
                    Metadata,
                    Status
                FROM ThirdPartyAccessTokens
                WHERE ProviderName = ? AND ServiceType = ? AND CompanyId = ? AND IsDeleted = 0
                ORDER BY ModifiedAt DESC
                """

                cursor.execute(query, (3, 3, company_id))
                row = cursor.fetchone()

                if row:
                    logging.info("Found Salesforce token for company: %s", company_id)

                    # Column names in order
                    columns = [
                        'Id', 'ProviderName', 'ServiceType', 'AccessToken', 'RefreshToken',
                        'ExpireAt', 'UserId', 'CreatedBy', 'CreatedAt', 'Version',
                        'ModifiedBy', 'ModifiedAt', 'IsDeleted', 'CompanyId', 'Metadata', 'Status'
                    ]

                    record = dict(zip(columns, row))

                    # Parse Metadata JSON safely
                    try:
                        record['Metadata'] = json.loads(record['Metadata']) if record['Metadata'] else {}
                    except json.JSONDecodeError:
                        logging.warning("Failed to parse Metadata JSON for company_id=%s", company_id)
                        record['Metadata'] = {}

                    return record

                # Fallback to ProviderName=2, ServiceType=2
                logging.info("No token found with ProviderName=3, ServiceType=3. Trying ProviderName=2, ServiceType=2")
                cursor.execute(query, (2, 2, company_id))
                row = cursor.fetchone()

                if row:
                    logging.info("Found Salesforce token with ProviderName=2, ServiceType=2 for company: %s", company_id)
                    record = dict(zip(columns, row))

                    try:
                        record['Metadata'] = json.loads(record['Metadata']) if record['Metadata'] else {}
                    except json.JSONDecodeError:
                        logging.warning("Failed to parse Metadata JSON for company_id=%s", company_id)
                        record['Metadata'] = {}

                    return record

                logging.warning("No Salesforce token found for company_id=%s", company_id)
                return None

        except Exception as e:
            logging.error("Error retrieving Salesforce token for company %s: %s", company_id, str(e), exc_info=True)
            return None
    def get_company_ids_from_db(self):
        try:
            with get_connection() as conn:  # Ensure get_connection is a callable
                cursor = conn.cursor()
                query = "SELECT Id FROM Companies"
                cursor.execute(query)
                result = cursor.fetchall()  # fetchall() is the correct method

                # Extract IDs from result
                company_ids = [row[0] for row in result]
                return company_ids

        except Exception as e:
            # Log the error or handle it appropriately
            print(f"Error fetching company IDs: {e}")
            return []
