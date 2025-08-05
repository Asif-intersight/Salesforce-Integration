# webhook_handlers.py
import xml.etree.ElementTree as ET
from datetime import datetime
import pytz
from config import ACCESS_TOKEN,INSTANCE_URL
from log_setup import webhook_logger
logger = webhook_logger()
class SalesforceWebhookHandler:
    def __init__(self, inserter, helper):
        self.inserter = inserter
        self.helper = helper
        
    def validate_webhook_signature(self, request, webhook_secret=None):
        """
        Validate webhook signature (optional but recommended for security)
        This is a placeholder - Salesforce outbound messages don't have signatures
        but you can implement your own validation
        """
        if not webhook_secret:
            return True 
            
        # For custom validation logic
        signature = request.headers.get('X-Salesforce-Signature', '')
        payload = request.data
        
        # Implement your validation logic here
        return True
    
    def parse_soap_request(self, xml_data):
        """
        Parse SOAP XML from Salesforce Outbound Messages
        """
        try:
            root = ET.fromstring(xml_data.decode('utf-8'))
            
            # Define namespaces
            namespaces = {
                'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
                'out': 'http://soap.sforce.com/2005/09/outbound',
                'sf': 'urn:sobject.enterprise.soap.sforce.com'
            }
            
            records = []
            notifications = root.findall('.//out:Notification', namespaces)
            
            for notification in notifications:
                sobjects = notification.findall('.//out:sObject', namespaces)
                for sobject in sobjects:
                    record = {}
                    # Extract all fields from the sObject
                    for field in sobject:
                        field_name = field.tag.split('}')[-1]  # Remove namespace
                        if field_name not in ['type', 'Id']:  # Skip metadata fields we'll handle separately
                            record[field_name] = field.text
                        elif field_name == 'Id':
                            record['Id'] = field.text
                    
                    # Get object type
                    type_elem = sobject.find('.//{urn:sobject.enterprise.soap.sforce.com}type')
                    if type_elem is not None:
                        record['ObjectType'] = type_elem.text
                        
                    records.append(record)
            
            return records
            
        except ET.ParseError as e:
            logger.error(f"Error parsing SOAP XML: {e}")
            return []
        except Exception as e:
            logger.error(f"Error processing SOAP request: {e}")
            return []
    
    def extract_records_from_payload(self, payload, payload_type='json'):
        """
        Extract records from webhook payload
        """
        if payload_type == 'soap':
            return payload  # Already parsed by parse_soap_request
        elif payload_type == 'json':
            # Handle JSON payload (for custom webhooks or platform events)
            if isinstance(payload, list):
                return payload
            elif isinstance(payload, dict):
                # Check common JSON structures
                if 'records' in payload:
                    return payload['records']
                elif 'data' in payload:
                    return payload['data']
                else:
                    return [payload]  # Single record
        return []
    
    def handle_account_change(self, payload):
        """Process account changes from webhook"""
        try:
            logger.info(f"Processing account payload: {payload}")
            logger.info(f"Payload type: {type(payload)}")
            
            # Auto-detect payload type
            payload_type = 'json'
            if isinstance(payload, list) and len(payload) > 0:
                # Check if it's already parsed SOAP data
                if isinstance(payload[0], dict) and 'ObjectType' in payload[0]:
                    payload_type = 'soap'
            
            logger.info(f"Detected payload type: {payload_type}")
            records = self.extract_records_from_payload(payload, payload_type)
            processed_count = 0
            
            if records:
                logger.info(f"Found {len(records)} records")
                # Filter only Account records
                if payload_type == 'soap':
                    account_records = [r for r in records if r.get('ObjectType') == 'Account']
                else:
                    # For JSON, assume all records are accounts if sent to accounts endpoint
                    account_records = records
                
                if account_records:
                    logger.info(f"Processing {len(account_records)} account records")
                    try:
                        self.inserter.insert_account(account_records)
                        processed_count = len(account_records)
                        
                        # Update sync log
                        self.helper.update_sync_log('accounts', datetime.now(pytz.UTC))
                        logger.info(f"Successfully processed {processed_count} account records via webhook")
                    except Exception as insert_error:
                        logger.error(f"Error inserting account records: {insert_error}")
                        return {"count": 0, "status": "error", "error": f"Database insert failed: {str(insert_error)}"}
                else:
                    logger.info("No account records found after filtering")
            else:
                logger.info("No records extracted from payload")
            
            return {"count": processed_count, "status": "success"}
            
        except Exception as e:
            logger.error(f"Error handling account change: {e}")
            import traceback
            traceback.logger.error_exc()
            return {"count": 0, "status": "error", "error": str(e)}
    def handle_contact_change(self, payload):
        """Process account changes from webhook"""
        try:
            logger.info(f"Processing contacts payload: {payload}")
            logger.info(f"Payload type: {type(payload)}")
            
            # Auto-detect payload type
            payload_type = 'json'
            if isinstance(payload, list) and len(payload) > 0:
                # Check if it's already parsed SOAP data
                if isinstance(payload[0], dict) and 'ObjectType' in payload[0]:
                    payload_type = 'soap'
            
            logger.info(f"Detected payload type: {payload_type}")
            records = self.extract_records_from_payload(payload, payload_type)
            processed_count = 0
            
            if records:
                logger.info(f"Found {len(records)} records")
                # Filter only Account records
                if payload_type == 'soap':
                    contact_records = [r for r in records if r.get('ObjectType') == 'Account']
                else:
                    # For JSON, assume all records are accounts if sent to accounts endpoint
                    contact_records = records
                
                if contact_records:
                    logger.info(f"Processing {len(contact_records)} contact records")
                    try:
                        self.inserter.upsert_contact(contact_records)
                        processed_count = len(contact_records)
                        
                        # Update sync log
                        self.helper.update_sync_log('contacts', datetime.now(pytz.UTC))
                        logger.info(f"Successfully processed {processed_count} contact records via webhook")
                    except Exception as insert_error:
                        logger.error(f"Error inserting contact records: {insert_error}")
                        return {"count": 0, "status": "error", "error": f"Database insert failed: {str(insert_error)}"}
                else:
                    logger.info("No account records found after filtering")
            else:
                logger.info("No records extracted from payload")
            
            return {"count": processed_count, "status": "success"}
            
        except Exception as e:
            logger.error(f"Error handling account change: {e}")
            import traceback
            traceback.print_exc()
            return {"count": 0, "status": "error", "error": str(e)}
    def handle_opportunity_change(self, payload):
        """Process opportunity changes from webhook"""
        try:
            logger.info(f"Processing account payload: {payload}")
            logger.info(f"Payload type: {type(payload)}")
            
            # Auto-detect payload type
            payload_type = 'json'
            if isinstance(payload, list) and len(payload) > 0:
                # Check if it's already parsed SOAP data
                if isinstance(payload[0], dict) and 'ObjectType' in payload[0]:
                    payload_type = 'soap'
            
            logger.info(f"Detected payload type: {payload_type}")
            records = self.extract_records_from_payload(payload, payload_type)
            processed_count = 0
            
            if records:
                logger.info(f"Found {len(records)} records")
                # Filter only Account records
                if payload_type == 'soap':
                    opportunity_records = [r for r in records if r.get('ObjectType') == 'Opportunity']
                else:
                    # For JSON, assume all records are accounts if sent to accounts endpoint
                    opportunity_records = records
                
                if opportunity_records:
                    logger.info(f"Processing {len(opportunity_records)} account records")
                    try:
                        for record in opportunity_records:
                            owner_id = record.get("OwnerId")
                            if owner_id:
                                owner_name, owner_email = self.fetch_owner_details(ACCESS_TOKEN, INSTANCE_URL, owner_id)
                                record["Owner"] = {"Name": owner_name, "Email": owner_email}
                        # self.inserter.upsert_opportunity(opportunity_records)
                  
                        processed_count = len(opportunity_records)
                        
                        # Update sync log
                        self.helper.update_sync_log('opportunities', datetime.now(pytz.UTC))
                        logger.info(f"Successfully processed {processed_count} opportunity records via webhook")
                    except Exception as insert_error:
                        logger.error(f"Error inserting opportunity records: {insert_error}")
                        return {"count": 0, "status": "error", "error": f"Database insert failed: {str(insert_error)}"}
                else:
                    logger.info("No account records found after filtering")
            else:
                logger.info("No records extracted from payload")
            
            return {"count": processed_count, "status": "success"}
            
        except Exception as e:
            logger.error(f"Error handling account change: {e}")
            import traceback
            traceback.print_exc()
            return {"count": 0, "status": "error", "error": str(e)}
        

    def handle_opportunity_activity_change(self, payload):
        """Process opportunity changes from webhook"""
        try:
            # print(f"Processing opportunity activities payload:::::::::::: {payload}")
            logger.info(f"Payload type: {type(payload)}")
            
            # Auto-detect payload type
            payload_type = 'json'
            if isinstance(payload, list) and len(payload) > 0:
                # Check if it's already parsed SOAP data
                if isinstance(payload[0], dict) and 'ObjectType' in payload[0]:
                    payload_type = 'soap'
            
            logger.info(f"Detected payload type: {payload_type}")
            records = self.extract_records_from_payload(payload, payload_type)
            processed_count = 0
            
            if records:
                logger.info(f"Found {len(records)} records")
                # Filter only opportunity Activities records
                if payload_type == 'soap':
                    opportunity_act_records = [r for r in records if r.get('ObjectType') == 'Tasks']
                else:
                    # For JSON, assume all records are accounts if sent to accounts endpoint
                    opportunity_act_records = records
                
                if opportunity_act_records:
                    logger.info(f"Processing {len(opportunity_act_records)} account records")
                    try:
                        self.inserter.upsert_opportunity_activities(opportunity_act_records)
                        # logger.info("------------------------------opportunity activites--------------------------------",opportunity_act_records)
                        processed_count = len(opportunity_act_records)
                        
                        # Update sync log
                        self.helper.update_sync_log('opportunity_activities', datetime.now(pytz.UTC))
                        logger.info(f"Successfully processed {processed_count} opportunity records via webhook")
                    except Exception as insert_error:
                        logger.error(f"Error inserting opportunity records: {insert_error}")
                        return {"count": 0, "status": "error", "error": f"Database insert failed: {str(insert_error)}"}
                else:
                    logger.info("No account records found after filtering")
            else:
                logger.info("No records extracted from payload")
            
            return {"count": processed_count, "status": "success"}
            
        except Exception as e:
            logger.error(f"Error handling account change: {e}")
            import traceback
            traceback.print_exc()
            return {"count": 0, "status": "error", "error": str(e)}
        
    def handle_users_change(self, payload):
        """Process opportunity changes from webhook"""
        try:
            # print(f"Processing User payload:::::::::::: {payload}")
            logger.info(f"Payload type: {type(payload)}")
            
            # Auto-detect payload type
            payload_type = 'json'
            if isinstance(payload, list) and len(payload) > 0:
                # Check if it's already parsed SOAP data
                if isinstance(payload[0], dict) and 'ObjectType' in payload[0]:
                    payload_type = 'soap'
            
            logger.info(f"Detected payload type: {payload_type}")
            records = self.extract_records_from_payload(payload, payload_type)
            processed_count = 0
            
            if records:
                logger.info(f"Found {len(records)} records")
                # Filter only opportunity Activities records
                if payload_type == 'soap':
                    users_records = [r for r in records if r.get('ObjectType') == 'Users']
                else:
                    # For JSON, assume all records are accounts if sent to accounts endpoint
                    users_records = records
                
                if users_records:
                    logger.info(f"Processing {len(users_records)} user records")
                    try:
                        self.inserter.upsert_user(users_records)
                        # print("------------------------------user Records--------------------------------",users_records)
                        processed_count = len(users_records)
                        
                        # Update sync log
                        self.helper.update_sync_log('opportunity_activities', datetime.now(pytz.UTC))
                        logger.info(f"Successfully processed {processed_count} user records via webhook")
                    except Exception as insert_error:
                        logger.error(f"Error inserting opportunity records: {insert_error}")
                        return {"count": 0, "status": "error", "error": f"Database insert failed: {str(insert_error)}"}
                else:
                    logger.info("No account records found after filtering")
            else:
                logger.info("No records extracted from payload")
            
            return {"count": processed_count, "status": "success"}
            
        except Exception as e:
            logger.error(f"Error handling account change: {e}")
            import traceback
            traceback.print_exc()
            return {"count": 0, "status": "error", "error": str(e)}


    
    def create_soap_acknowledgment(self, success=True):
            """Create SOAP acknowledgment response for Salesforce"""
            ack_value = "true" if success else "false"
            
            return f'''<?xml version="1.0" encoding="utf-8"?>
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
        <soapenv:Body>
            <notificationsResponse xmlns="http://soap.sforce.com/2005/09/outbound">
                <Ack>{ack_value}</Ack>
            </notificationsResponse>
        </soapenv:Body>
    </soapenv:Envelope>'''


    def fetch_owner_details(self, access_token, instance_url, owner_id):
        from services.salesforce_client import execute_soql_query

        if not owner_id:
            return None, None

        query = f"SELECT Name, Email FROM User WHERE Id = '{owner_id}'"
        try:
            result = execute_soql_query(access_token, instance_url, query)
            logger.info("Owner Query Result:", result)

            # If result is a dict with records key
            if isinstance(result, dict) and 'records' in result:
                if result['records']:
                    owner = result['records'][0]
                    return owner.get('Name'), owner.get('Email')

            # If result is a list
            elif isinstance(result, list) and len(result) > 0:
                owner = result[0]
                return owner.get('Name'), owner.get('Email')

        except Exception as e:
            logger.error(f"Error fetching Owner details for {owner_id}: {e}")

        return None, None
