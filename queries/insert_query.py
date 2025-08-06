from database.connection_manager import get_connection
from datetime import datetime,time,timezone
from log_setup import simple_logger
import logging
import pyodbc
logger = simple_logger()
class SalesforceUpsertFunctions:
    def __init__(self):
        pass
    def upsert_opportunity(self, records):
        logging.info("Starting Upsertion for Salesforce Opportunities")
       
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                for idx, rec in enumerate(records, start=1):
                    try:
                        logging.debug(f"Processing record {idx}/{len(records)}: Id={rec.get('Id')}")
                        
                        query = """
                            MERGE INTO dbo.SalesforceOpportunities AS Target
                            USING (SELECT ? AS SalesforceId) AS Source
                            ON Target.SalesforceId = Source.SalesforceId
                            WHEN MATCHED THEN 
                                UPDATE SET
                                    IsDeleted = ?, AccountId = ?, IsPrivate = ?, Name = ?, Description = ?,
                                    StageName = ?, Amount = ?, Probability = ?, ExpectedRevenue = ?, TotalOpportunityQuantity = ?,
                                    CloseDate = ?, Type = ?, LeadSource = ?, IsClosed = ?, IsWon = ?, ForecastCategory = ?,
                                    ForecastCategoryName = ?, OwnerId = ?, OwnerName = ?, CreatedDate = ?, CreatedById = ?,
                                    LastModifiedDate = ?, LastModifiedById = ?, PushCount = ?, LastStageChangeDate = ?,
                                    FiscalQuarter = ?, FiscalYear = ?, Fiscal = ?, ContactId = ?, LastViewedDate = ?,
                                    LastReferenceDate = ?, HasOpenActivity = ?, HasOverdueTask = ?, DeliveryInstallationStatus = ?,
                                    OrderNumber = ?, CurrentGenerators = ?, MainCompetitors = ?, OwnerEmail = ? 
                            WHEN NOT MATCHED THEN
                                INSERT (
                                    SalesforceId, IsDeleted, AccountId, IsPrivate, Name, Description,
                                    StageName, Amount, Probability, ExpectedRevenue, TotalOpportunityQuantity,
                                    CloseDate, Type, LeadSource, IsClosed, IsWon, ForecastCategory,
                                    ForecastCategoryName, OwnerId, OwnerName, CreatedDate, CreatedById,
                                    LastModifiedDate, LastModifiedById, PushCount, LastStageChangeDate,
                                    FiscalQuarter, FiscalYear, Fiscal, ContactId, LastViewedDate,
                                    LastReferenceDate, HasOpenActivity, HasOverdueTask,
                                    DeliveryInstallationStatus, OrderNumber, CurrentGenerators,
                                    MainCompetitors, OwnerEmail
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """

                        values = [
                            rec.get('Id'),  # Used in USING

                            # UPDATE values
                            rec.get('IsDeleted'),
                            rec.get('AccountId'),
                            rec.get('IsPrivate'), 
                            rec.get('Name'), 
                            rec.get('Description'),
                            rec.get('StageName'), 
                            rec.get('Amount'), 
                            rec.get('Probability'), 
                            rec.get('ExpectedRevenue'), 
                            rec.get('TotalOpportunityQuantity'),
                            rec.get('CloseDate'), 
                            rec.get('Type'), 
                            rec.get('LeadSource'), 
                            rec.get('IsClosed'),
                            rec.get('IsWon'),
                            rec.get('ForecastCategory'),
                            rec.get('ForecastCategoryName'),
                            rec.get('OwnerId'),
                            rec.get('Owner', {}).get('Name'),
                            self.safe_parse_date(rec.get('CreatedDate')),
                            rec.get('CreatedById'),
                            self.safe_parse_date(rec.get('LastModifiedDate')), 
                            rec.get('LastModifiedById'), 
                            rec.get('PushCount'), 
                            self.safe_parse_date(rec.get('LastStageChangeDate')),
                            rec.get('FiscalQuarter'), 
                            rec.get('FiscalYear'), 
                            rec.get('Fiscal'), 
                            rec.get('ContactId'), 
                            self.safe_parse_date(rec.get('LastViewedDate')),
                            self.safe_parse_date(rec.get('LastReferenceDate')), 
                            rec.get('HasOpenActivity'), 
                            rec.get('HasOverdueTask'), 
                            rec.get('DeliveryInstallationStatus'),
                            rec.get('OrderNumber'), 
                            rec.get('CurrentGenerators'), 
                            rec.get('MainCompetitors'), 
                            rec.get('Owner', {}).get('Email'),

                            # INSERT values
                            rec.get('Id'),
                            rec.get('IsDeleted'),
                            rec.get('AccountId'),
                            rec.get('IsPrivate'),
                            rec.get('Name'),
                            rec.get('Description'),
                            rec.get('StageName'),
                            rec.get('Amount'),
                            rec.get('Probability'),
                            rec.get('ExpectedRevenue'),
                            rec.get('TotalOpportunityQuantity'),
                            rec.get('CloseDate'),
                            rec.get('Type'),
                            rec.get('LeadSource'),
                            rec.get('IsClosed'),
                            rec.get('IsWon'),
                            rec.get('ForecastCategory'),
                            rec.get('ForecastCategoryName'),
                            rec.get('OwnerId'),
                            rec.get('Owner', {}).get('Name'),
                            self.safe_parse_date(rec.get('CreatedDate')),
                            rec.get('CreatedById'),
                            self.safe_parse_date(rec.get('LastModifiedDate')),
                            rec.get('LastModifiedById'),
                            rec.get('PushCount'),
                            self.safe_parse_date(rec.get('LastStageChangeDate')),
                            rec.get('FiscalQuarter'),
                            rec.get('FiscalYear'),
                            rec.get('Fiscal'),
                            rec.get('ContactId'),
                            self.safe_parse_date(rec.get('LastViewedDate')),
                            self.safe_parse_date(rec.get('LastReferenceDate')),
                            rec.get('HasOpenActivity'),
                            rec.get('HasOverdueTask'),
                            rec.get('DeliveryInstallationStatus'),
                            rec.get('OrderNumber'),
                            rec.get('CurrentGenerators'),
                            rec.get('MainCompetitors'),
                            rec.get('Owner', {}).get('Email')
                        ]

                        cursor.execute(query, values)

                    except pyodbc.DatabaseError as db_err:
                        logging.error(f"Database error while processing record Id={rec.get('Id')}: {db_err}", exc_info=True)
                        logger.error(f"Database error while processing record Id={rec.get('Id')}: {db_err}", exc_info=True)
                    except Exception as ex:
                        logging.error(f"Unexpected error while processing record Id={rec.get('Id')}: {ex}", exc_info=True)
                        logger.error(f"Unexpected error while processing record Id={rec.get('Id')}: {ex}", exc_info=True)

                conn.commit()
                logging.info("Upsertion completed successfully.")

        except pyodbc.InterfaceError as conn_err:
            logging.critical(f"Failed to connect to the database: {conn_err}", exc_info=True)
            logger.critical(f"Failed to connect to the database: {conn_err}", exc_info=True)
        except Exception as ex:
            logging.critical(f"Unexpected error during upsert operation: {ex}", exc_info=True)
            logger.critical(f"Unexpected error during upsert operation: {ex}", exc_info=True)


    def safe_parse_date(self, value):
        """
        Parses date/time strings, converts 'YYYY-MM-DD' into midnight datetime,
        and returns a naive UTC datetime or None if invalid.
        """
        if not value:
            return None

        # If it's already a datetime object, return it
        if isinstance(value, datetime):
            # Convert timezone-aware datetime to naive UTC if needed
            if value.tzinfo is not None:
                return value.astimezone(timezone.utc).replace(tzinfo=None)
            return value

        # Convert to string if not already
        if not isinstance(value, str):
            return None

        # Try standard full datetime formats
        # Added the missing format that .isoformat() produces
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z",      # With timezone and microseconds
                    "%Y-%m-%dT%H:%M:%S%z",          # With timezone, no microseconds
                    "%Y-%m-%dT%H:%M:%S.%f",         # No timezone, with microseconds (isoformat() output)
                    "%Y-%m-%dT%H:%M:%S",            # No timezone, no microseconds
                    "%Y-%m-%d"):                    # Date only
            try:
                dt = datetime.strptime(value, fmt)
                
                # If date-only, add midnight time
                if fmt == "%Y-%m-%d":
                    dt = datetime.combine(dt.date(), time.min)
                
                # Convert timezone-aware datetime to naive UTC
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                
                return dt
            except ValueError:
                continue

        # Handle impossible dates like "2024-12-34"
        try:
            parts = value.split("T")[0].split("-")
            if len(parts) == 3:
                y, m, d = map(int, parts)
                _ = datetime(year=y, month=m, day=d)  # Will raise if invalid
        except Exception:
            print(f"Invalid date encountered: '{value}' → storing as NULL")

        return None
    
    def insert_account(self, records):
        logging.info("Starting Upsertion for Salesforce Accounts")
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                for idx, rec in enumerate(records, start=1):
                    try:
                        logging.debug(f"Processing account {idx}/{len(records)}: Id={rec.get('Id')}")

                        query = """
                            MERGE INTO dbo.SalesForceAccounts AS Target
                            USING (SELECT ? AS SalesForceId) AS Source
                            ON Target.SalesForceId = Source.SalesForceId
                            WHEN MATCHED THEN 
                                UPDATE SET
                                    Name = ?, Type = ?, BillingStreet = ?, BillingCity = ?, BillingState = ?, BillingPostalCode = ?, BillingCountry = ?,
                                    ShippingStreet = ?, ShippingCity = ?, ShippingState = ?, ShippingPostalCode = ?, ShippingCountry = ?,
                                    Phone = ?, Fax = ?, AccountNumber = ?, Website = ?, PhotoUrl = ?, Sic = ?, Industry = ?, AnnualRevenue = ?,
                                    NumberOfEmployees = ?, Ownership = ?, TickerSymbol = ?, Description = ?, Rating = ?, OwnerId = ?,
                                    CreatedDate = ?, CreatedById = ?, LastModifiedDate = ?, LastModifiedById = ?, CleanStatus = ?,
                                    AccountSource = ?, DunsNumber = ?, TradeStyle = ?, CustomerPriority = ?, SLA = ?, NumberOfLocations = ?,
                                    UpSellOpportunity = ?, SLASerialNumber = ?, SLAExpirationDate = ?, IsDeleted = ?
                            WHEN NOT MATCHED THEN
                                INSERT (
                                    SalesForceId, Name, Type, BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry,
                                    ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry,
                                    Phone, Fax, AccountNumber, Website, PhotoUrl, Sic, Industry, AnnualRevenue,
                                    NumberOfEmployees, Ownership, TickerSymbol, Description, Rating, OwnerId,
                                    CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, CleanStatus,
                                    AccountSource, DunsNumber, TradeStyle, CustomerPriority, SLA, NumberOfLocations,
                                    UpSellOpportunity, SLASerialNumber, SLAExpirationDate, IsDeleted
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """

                        values = [
                            rec.get('Id'),  # For USING

                            # UPDATE values
                            rec.get('Name'),
                            rec.get('Type'),
                            rec.get('BillingStreet'),
                            rec.get('BillingCity'),
                            rec.get('BillingState'),
                            rec.get('BillingPostalCode'),
                            rec.get('BillingCountry'),
                            rec.get('ShippingStreet'),
                            rec.get('ShippingCity'),
                            rec.get('ShippingState'),
                            rec.get('ShippingPostalCode'),
                            rec.get('ShippingCountry'),
                            rec.get('Phone'),
                            rec.get('Fax'),
                            rec.get('AccountNumber'),
                            rec.get('Website'),
                            rec.get('PhotoUrl'),
                            rec.get('Sic'),
                            rec.get('Industry'),
                            rec.get('AnnualRevenue'),
                            rec.get('NumberOfEmployees'),
                            rec.get('Ownership'),
                            rec.get('TickerSymbol'),
                            rec.get('Description'),
                            rec.get('Rating'),
                            rec.get('OwnerId'),
                            self.safe_parse_date(rec.get('CreatedDate')),
                            rec.get('CreatedById'),
                            self.safe_parse_date(rec.get('LastModifiedDate')),
                            rec.get('LastModifiedById'),
                            rec.get('CleanStatus'),
                            rec.get('AccountSource'),
                            rec.get('DunsNumber'),
                            rec.get('TradeStyle__c'),
                            rec.get('CustomerPriority__c'),
                            rec.get('SLA__c'),
                            rec.get('NumberOfLocations__c'),
                            rec.get('UpSellOpportunity__c'),
                            rec.get('SLASerialNumber__c'),
                            self.safe_parse_date(rec.get('SLAExpirationDate__c')),
                            rec.get('IsDeleted'),

                            # INSERT values (same order)
                            rec.get('Id'),
                            rec.get('Name'),
                            rec.get('Type'),
                            rec.get('BillingStreet'),
                            rec.get('BillingCity'),
                            rec.get('BillingState'),
                            rec.get('BillingPostalCode'),
                            rec.get('BillingCountry'),
                            rec.get('ShippingStreet'),
                            rec.get('ShippingCity'),
                            rec.get('ShippingState'),
                            rec.get('ShippingPostalCode'),
                            rec.get('ShippingCountry'),
                            rec.get('Phone'),
                            rec.get('Fax'),
                            rec.get('AccountNumber'),
                            rec.get('Website'),
                            rec.get('PhotoUrl'),
                            rec.get('Sic'),
                            rec.get('Industry'),
                            rec.get('AnnualRevenue'),
                            rec.get('NumberOfEmployees'),
                            rec.get('Ownership'),
                            rec.get('TickerSymbol'),
                            rec.get('Description'),
                            rec.get('Rating'),
                            rec.get('OwnerId'),
                            self.safe_parse_date(rec.get('CreatedDate')),
                            rec.get('CreatedById'),
                            self.safe_parse_date(rec.get('LastModifiedDate')),
                            rec.get('LastModifiedById'),
                            rec.get('CleanStatus'),
                            rec.get('AccountSource'),
                            rec.get('DunsNumber'),
                            rec.get('TradeStyle__c'),
                            rec.get('CustomerPriority__c'),
                            rec.get('SLA__c'),
                            rec.get('NumberOfLocations__c'),
                            rec.get('UpSellOpportunity__c'),
                            rec.get('SLASerialNumber__c'),
                            self.safe_parse_date(rec.get('SLAExpirationDate__c')),
                            rec.get('IsDeleted')
                        ]

                        cursor.execute(query, values)

                    except pyodbc.DatabaseError as db_err:
                        logging.error(f"Database error while processing account Id={rec.get('Id')}: {db_err}", exc_info=True)
                    except Exception as ex:
                        logging.error(f"Unexpected error while processing account Id={rec.get('Id')}: {ex}", exc_info=True)

                conn.commit()
                logging.info("Account upsertion completed successfully.")

        except pyodbc.InterfaceError as conn_err:
            logging.critical(f"Database connection failed: {conn_err}", exc_info=True)
        except Exception as ex:
            logging.critical(f"Unexpected error during account upsertion: {ex}", exc_info=True)



    def upsert_contact(self, records):
        logging.info("Starting upsertion for Salesforce Contacts.")
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                for idx, rec in enumerate(records, start=1):
                    try:
                        logging.debug(f"Processing contact {idx}/{len(records)}: Id={rec.get('Id')}")
                        
                        query = """
                            MERGE INTO dbo.SalesforceContacts AS Target
                            USING (SELECT ? AS SalesforceId) AS Source
                            ON Target.SalesforceId = Source.SalesforceId
                            WHEN MATCHED THEN
                                UPDATE SET
                                    IsDeleted = ?, MasterRecordId = ?, AccountId = ?, LastName = ?,
                                    FirstName = ?, Salutation = ?, Name = ?, OtherStreet = ?, OtherCity = ?,
                                    OtherState = ?, OtherPostalCode = ?, OtherCountry = ?, MailingStreet = ?, MailingCity = ?,
                                    MailingState = ?, MailingPostalCode = ?, MailingCountry = ?, Phone = ?, Fax = ?,
                                    MobilePhone = ?, HomePhone = ?, OtherPhone = ?, AssistantPhone = ?, Email = ?,
                                    Title = ?, Department = ?, AssistantName = ?, LeadSource = ?, Description = ?,
                                    OwnerId = ?, CreatedDate = ?, LastModifiedDate = ?, LastActivityDate = ?, LastCURequestDate = ?,
                                    LastCUUpdateDate = ?, LastViewedDate = ?, LastReferencedDate = ?, EmailBouncedReason = ?, EmailBouncedDate = ?,
                                    IsEmailBounced = ?, PhotoUrl = ?, Jigsaw = ?, JigsawContactId = ?, CleanStatus = ?,
                                    IndividualId = ?, Level__c = ?, Languages__c = ?
                            WHEN NOT MATCHED THEN
                                INSERT (
                                    SalesforceId, IsDeleted, MasterRecordId, AccountId, LastName,
                                    FirstName, Salutation, Name, OtherStreet, OtherCity,
                                    OtherState, OtherPostalCode, OtherCountry, MailingStreet, MailingCity,
                                    MailingState, MailingPostalCode, MailingCountry, Phone, Fax,
                                    MobilePhone, HomePhone, OtherPhone, AssistantPhone, Email,
                                    Title, Department, AssistantName, LeadSource, Description,
                                    OwnerId, CreatedDate, LastModifiedDate, LastActivityDate, LastCURequestDate,
                                    LastCUUpdateDate, LastViewedDate, LastReferencedDate, EmailBouncedReason, EmailBouncedDate,
                                    IsEmailBounced, PhotoUrl, Jigsaw, JigsawContactId, CleanStatus,
                                    IndividualId, Level__c, Languages__c
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """

                        values = [
                            rec.get("Id"),

                            # UPDATE values
                            rec.get("IsDeleted"), rec.get("MasterRecordId"), rec.get("AccountId"),
                            rec.get("LastName"), rec.get("FirstName"), rec.get("Salutation"), rec.get("Name"),
                            rec.get("OtherStreet"), rec.get("OtherCity"), rec.get("OtherState"),
                            rec.get("OtherPostalCode"), rec.get("OtherCountry"),
                            rec.get("MailingStreet"), rec.get("MailingCity"), rec.get("MailingState"),
                            rec.get("MailingPostalCode"), rec.get("MailingCountry"),
                            rec.get("Phone"), rec.get("Fax"), rec.get("MobilePhone"),
                            rec.get("HomePhone"), rec.get("OtherPhone"), rec.get("AssistantPhone"),
                            rec.get("Email"), rec.get("Title"), rec.get("Department"), rec.get("AssistantName"),
                            rec.get("LeadSource"), rec.get("Description"), rec.get("OwnerId"),
                            self.safe_parse_date(rec.get("CreatedDate")),
                            self.safe_parse_date(rec.get("LastModifiedDate")),
                            self.safe_parse_date(rec.get("LastActivityDate")),
                            self.safe_parse_date(rec.get("LastCURequestDate")),
                            self.safe_parse_date(rec.get("LastCUUpdateDate")),
                            self.safe_parse_date(rec.get("LastViewedDate")),
                            self.safe_parse_date(rec.get("LastReferencedDate")),
                            rec.get("EmailBouncedReason"),
                            self.safe_parse_date(rec.get("EmailBouncedDate")),
                            rec.get("IsEmailBounced"),
                            rec.get("PhotoUrl"), rec.get("Jigsaw"), rec.get("JigsawContactId"),
                            rec.get("CleanStatus"), rec.get("IndividualId"),
                            rec.get("Level__c"), rec.get("Languages__c"),

                            # INSERT values (same order)
                            rec.get("Id"),
                            rec.get("IsDeleted"), rec.get("MasterRecordId"), rec.get("AccountId"),
                            rec.get("LastName"), rec.get("FirstName"), rec.get("Salutation"), rec.get("Name"),  # ✅ FIXED: Name added here
                            rec.get("OtherStreet"), rec.get("OtherCity"), rec.get("OtherState"),
                            rec.get("OtherPostalCode"), rec.get("OtherCountry"),
                            rec.get("MailingStreet"), rec.get("MailingCity"), rec.get("MailingState"),
                            rec.get("MailingPostalCode"), rec.get("MailingCountry"),
                            rec.get("Phone"), rec.get("Fax"), rec.get("MobilePhone"),
                            rec.get("HomePhone"), rec.get("OtherPhone"), rec.get("AssistantPhone"),
                            rec.get("Email"), rec.get("Title"), rec.get("Department"), rec.get("AssistantName"),
                            rec.get("LeadSource"), rec.get("Description"), rec.get("OwnerId"),
                            self.safe_parse_date(rec.get("CreatedDate")),
                            self.safe_parse_date(rec.get("LastModifiedDate")),
                            self.safe_parse_date(rec.get("LastActivityDate")),
                            self.safe_parse_date(rec.get("LastCURequestDate")),
                            self.safe_parse_date(rec.get("LastCUUpdateDate")),
                            self.safe_parse_date(rec.get("LastViewedDate")),
                            self.safe_parse_date(rec.get("LastReferencedDate")),
                            rec.get("EmailBouncedReason"),
                            self.safe_parse_date(rec.get("EmailBouncedDate")),
                            rec.get("IsEmailBounced"),
                            rec.get("PhotoUrl"), rec.get("Jigsaw"), rec.get("JigsawContactId"),
                            rec.get("CleanStatus"), rec.get("IndividualId"),
                            rec.get("Level__c"), rec.get("Languages__c")
                        ]

                        cursor.execute(query, values)

                    except pyodbc.DatabaseError as db_err:
                        logging.error(f"Database error while upserting contact Id={rec.get('Id')}: {db_err}", exc_info=True)
                    except Exception as ex:
                        logging.error(f"Unexpected error while upserting contact Id={rec.get('Id')}: {ex}", exc_info=True)

                conn.commit()
                logging.info("Contact upsertion completed successfully.")

        except pyodbc.InterfaceError as conn_err:
            logging.critical(f"Failed to connect to the database: {conn_err}", exc_info=True)
        except Exception as ex:
            logging.critical(f"Unexpected error during contact upsertion: {ex}", exc_info=True)

    def upsert_opportunity_activities(self, records):
        logging.info("Starting upsertion for Salesforce Opportunity Activities.")
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                for idx, rec in enumerate(records, start=1):
                    try:
                        logging.debug(f"Processing opportunity activity {idx}/{len(records)}: Id={rec.get('Id')}")

                        query = """
                            MERGE INTO dbo.SalesforceOpportunityActivities AS Target
                            USING (SELECT ? AS SalesforceId) AS Source
                            ON Target.SalesforceId = Source.SalesforceId
                            WHEN MATCHED THEN
                                UPDATE SET
                                    WhatId = ?, WhoId = ?, AccountId = ?, Subject = ?, ActivityDate = ?, Status = ?,
                                    Priority = ?, OwnerId = ?, CreatedDate = ?, IsClosed = ?, IsDeleted = ?,
                                    CallType = ?, CallDisposition = ?, CallObject = ?, CreatedById = ?,
                                    Description = ?, CompletedDateTime = ?, TaskSubType = ?
                            WHEN NOT MATCHED THEN
                                INSERT (
                                    SalesforceId, WhatId, WhoId, AccountId, Subject, ActivityDate, Status,
                                    Priority, OwnerId, CreatedDate, IsClosed, IsDeleted,
                                    CallType, CallDisposition, CallObject, CreatedById,
                                    Description, CompletedDateTime, TaskSubType
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """

                        values = [
                            rec.get('Id'),  # For USING clause

                            # --- UPDATE values ---
                            rec.get('WhatId'),
                            rec.get('WhoId'),
                            rec.get('AccountId'),
                            rec.get('Subject'),
                            self.safe_parse_date(rec.get('ActivityDate')),
                            rec.get('Status'),
                            rec.get('Priority'),
                            rec.get('OwnerId'),
                            self.safe_parse_date(rec.get('CreatedDate')),
                            rec.get('IsClosed'),
                            rec.get('IsDeleted'),
                            rec.get('CallType'),
                            rec.get('CallDisposition'),
                            rec.get('CallObject'),
                            rec.get('CreatedById'),
                            rec.get('Description'),
                            self.safe_parse_date(rec.get('CompletedDateTime')),
                            rec.get('TaskSubType'),

                            # --- INSERT values ---
                            rec.get('Id'),
                            rec.get('WhatId'),
                            rec.get('WhoId'),
                            rec.get('AccountId'),
                            rec.get('Subject'),
                            self.safe_parse_date(rec.get('ActivityDate')),
                            rec.get('Status'),
                            rec.get('Priority'),
                            rec.get('OwnerId'),
                            self.safe_parse_date(rec.get('CreatedDate')),
                            rec.get('IsClosed'),
                            rec.get('IsDeleted'),
                            rec.get('CallType'),
                            rec.get('CallDisposition'),
                            rec.get('CallObject'),
                            rec.get('CreatedById'),
                            rec.get('Description'),
                            self.safe_parse_date(rec.get('CompletedDateTime')),
                            rec.get('TaskSubType')
                        ]

                        cursor.execute(query, values)

                    except pyodbc.DatabaseError as db_err:
                        logging.error(f"Database error while processing activity Id={rec.get('Id')}: {db_err}", exc_info=True)
                    except Exception as ex:
                        logging.error(f"Unexpected error while processing activity Id={rec.get('Id')}: {ex}", exc_info=True)

                conn.commit()
                logging.info("All opportunity activities upserted successfully.")

        except pyodbc.InterfaceError as conn_err:
            logging.critical(f"Failed to connect to the database: {conn_err}", exc_info=True)
        except Exception as ex:
            logging.critical(f"Unexpected error during opportunity activity upsertion: {ex}", exc_info=True)




    def upsert_opp_history(self, records):
        logging.info("Starting upsertion for Salesforce Opportunity History records.")
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                for idx, rec in enumerate(records, start=1):
                    try:
                        logging.debug(f"Processing opportunity history {idx}/{len(records)}: Id={rec.get('Id')}")

                        query = """
                            MERGE INTO dbo.SalesForceOpportunityHistory AS Target
                            USING (SELECT ? AS SalesforceId) AS Source
                            ON Target.SalesforceId = Source.SalesforceId
                            WHEN MATCHED THEN
                                UPDATE SET
                                    OpportunityId = ?, CreatedDate = ?, StageName = ?, Amount = ?, ExpectedRevenue = ?, CloseDate = ?,
                                    Probability = ?, ForecastCategory = ?, IsDeleted = ?, PrevAmount = ?, PrevCloseDate = ?
                            WHEN NOT MATCHED THEN
                                INSERT (
                                    SalesforceId, OpportunityId, CreatedDate, StageName, Amount, ExpectedRevenue, CloseDate,
                                    Probability, ForecastCategory, IsDeleted, PrevAmount, PrevCloseDate
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """

                        values = [
                            rec.get('Id'),

                            # UPDATE values
                            rec.get('OpportunityId'),
                            self.safe_parse_date(rec.get('CreatedDate')),
                            rec.get('StageName'),
                            rec.get('Amount'),
                            rec.get('ExpectedRevenue'),
                            self.safe_parse_date(rec.get('CloseDate')),
                            rec.get('Probability'),
                            rec.get('ForecastCategory'),
                            rec.get('IsDeleted'),
                            rec.get('PrevAmount'),
                            self.safe_parse_date(rec.get('PrevCloseDate')),

                            # INSERT values
                            rec.get('Id'),
                            rec.get('OpportunityId'),
                            self.safe_parse_date(rec.get('CreatedDate')),
                            rec.get('StageName'),
                            rec.get('Amount'),
                            rec.get('ExpectedRevenue'),
                            self.safe_parse_date(rec.get('CloseDate')),
                            rec.get('Probability'),
                            rec.get('ForecastCategory'),
                            rec.get('IsDeleted'),
                            rec.get('PrevAmount'),
                            self.safe_parse_date(rec.get('PrevCloseDate'))
                        ]

                        cursor.execute(query, values)

                    except pyodbc.DatabaseError as db_err:
                        logging.error(f"Database error while upserting opportunity history Id={rec.get('Id')}: {db_err}", exc_info=True)
                    except Exception as ex:
                        logging.error(f"Unexpected error while upserting opportunity history Id={rec.get('Id')}: {ex}", exc_info=True)

                conn.commit()
                logging.info("Opportunity history upsertion completed successfully.")

        except pyodbc.InterfaceError as conn_err:
            logging.critical(f"Database connection failed: {conn_err}", exc_info=True)
        except Exception as ex:
            logging.critical(f"Unexpected error during opportunity history upsertion: {ex}", exc_info=True)

    def upsert_user(self, records):
        logging.info("Starting upsertion for Salesforce Users.")
        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                for idx, rec in enumerate(records, start=1):
                    try:
                        logging.debug(f"Processing user {idx}/{len(records)}: Id={rec.get('Id')}")

                        query = """
                            MERGE INTO dbo.SalesforceUsers AS Target
                            USING (SELECT ? AS Id) AS Source
                            ON Target.Id = Source.Id
                            WHEN MATCHED THEN
                                UPDATE SET
                                    Username = ?, LastName = ?, FirstName = ?, Name = ?, CompanyName = ?, Division = ?,
                                    Department = ?, Title = ?, Street = ?, City = ?, State = ?, PostalCode = ?, Country = ?, Email = ?,
                                    EmailPreferencesAutoBcc = ?, EmailPreferencesAutoBccStayInTouch = ?, EmailPreferencesStayInTouchReminder = ?,
                                    SenderEmail = ?, SenderName = ?, MobilePhone = ?, Alias = ?, CommunityNickname = ?, BadgeText = ?, IsActive = ?,
                                    TimeZoneSidKey = ?, UserRoleId = ?, LocaleSidKey = ?, ReceivesInfoEmails = ?, EmailEncodingKey = ?, ProfileId = ?,
                                    UserType = ?, LanguageLocaleKey = ?, EmployeeNumber = ?, CreatedDate = ?, CreatedById = ?, LastModifiedDate = ?,
                                    LastModifiedById = ?, AboutMe = ?
                            WHEN NOT MATCHED THEN
                                INSERT (
                                    Id, Username, LastName, FirstName, Name, CompanyName, Division, Department, Title, Street, City, State,
                                    PostalCode, Country, Email, EmailPreferencesAutoBcc, EmailPreferencesAutoBccStayInTouch,
                                    EmailPreferencesStayInTouchReminder, SenderEmail, SenderName, MobilePhone, Alias, CommunityNickname,
                                    BadgeText, IsActive, TimeZoneSidKey, UserRoleId, LocaleSidKey, ReceivesInfoEmails, EmailEncodingKey,
                                    ProfileId, UserType, LanguageLocaleKey, EmployeeNumber, CreatedDate, CreatedById, LastModifiedDate,
                                    LastModifiedById, AboutMe
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """

                        values = [
                            rec.get("Id"),  # For USING

                            # --- UPDATE values ---
                            rec.get("Username"),
                            rec.get("LastName"),
                            rec.get("FirstName"),
                            rec.get("Name"),
                            rec.get("CompanyName"),
                            rec.get("Division"),
                            rec.get("Department"),
                            rec.get("Title"),
                            rec.get("Street"),
                            rec.get("City"),
                            rec.get("State"),
                            rec.get("PostalCode"),
                            rec.get("Country"),
                            rec.get("Email"),
                            rec.get("EmailPreferencesAutoBcc"),
                            rec.get("EmailPreferencesAutoBccStayInTouch"),
                            rec.get("EmailPreferencesStayInTouchReminder"),
                            rec.get("SenderEmail"),
                            rec.get("SenderName"),
                            rec.get("MobilePhone"),
                            rec.get("Alias"),
                            rec.get("CommunityNickname"),
                            rec.get("BadgeText"),
                            rec.get("IsActive"),
                            rec.get("TimeZoneSidKey"),
                            rec.get("UserRoleId"),
                            rec.get("LocaleSidKey"),
                            rec.get("ReceivesInfoEmails"),  # Ensure this matches your DB schema
                            rec.get("EmailEncodingKey"),
                            rec.get("ProfileId"),
                            rec.get("UserType"),
                            rec.get("LanguageLocaleKey"),
                            rec.get("EmployeeNumber"),
                            self.safe_parse_date(rec.get("CreatedDate")),
                            rec.get("CreatedById"),
                            self.safe_parse_date(rec.get("LastModifiedDate")),
                            rec.get("LastModifiedById"),
                            rec.get("AboutMe"),

                            # --- INSERT values ---
                            rec.get("Id"),
                            rec.get("Username"),
                            rec.get("LastName"),
                            rec.get("FirstName"),
                            rec.get("Name"),
                            rec.get("CompanyName"),
                            rec.get("Division"),
                            rec.get("Department"),
                            rec.get("Title"),
                            rec.get("Street"),
                            rec.get("City"),
                            rec.get("State"),
                            rec.get("PostalCode"),
                            rec.get("Country"),
                            rec.get("Email"),
                            rec.get("EmailPreferencesAutoBcc"),
                            rec.get("EmailPreferencesAutoBccStayInTouch"),
                            rec.get("EmailPreferencesStayInTouchReminder"),
                            rec.get("SenderEmail"),
                            rec.get("SenderName"),
                            rec.get("MobilePhone"),
                            rec.get("Alias"),
                            rec.get("CommunityNickname"),
                            rec.get("BadgeText"),
                            rec.get("IsActive"),
                            rec.get("TimeZoneSidKey"),
                            rec.get("UserRoleId"),
                            rec.get("LocaleSidKey"),
                            rec.get("ReceivesInfoEmails"),
                            rec.get("EmailEncodingKey"),
                            rec.get("ProfileId"),
                            rec.get("UserType"),
                            rec.get("LanguageLocaleKey"),
                            rec.get("EmployeeNumber"),
                            self.safe_parse_date(rec.get("CreatedDate")),
                            rec.get("CreatedById"),
                            self.safe_parse_date(rec.get("LastModifiedDate")),
                            rec.get("LastModifiedById"),
                            rec.get("AboutMe")
                        ]

                        cursor.execute(query, values)

                    except pyodbc.DatabaseError as db_err:
                        logging.error(f"Database error while upserting user Id={rec.get('Id')}: {db_err}", exc_info=True)
                    except Exception as ex:
                        logging.error(f"Unexpected error while upserting user Id={rec.get('Id')}: {ex}", exc_info=True)

                conn.commit()
                logging.info("All Salesforce Users upserted successfully.")

        except pyodbc.InterfaceError as conn_err:
            logging.critical(f"Database connection failed: {conn_err}", exc_info=True)
        except Exception as ex:
            logging.critical(f"Unexpected error during user upsertion: {ex}", exc_info=True)
            
    def upsert_call_stages(self, records):
        """
        Upserts records into dbo.SalesforceCallStages.
        Each record should contain:
        - 'Id' (SalesforceId)
        - 'IsActive'
        - 'MasterLabel' (CallStage)
        - 'SortOrder'
        """
        logging.info("Starting upsertion for Salesforce Call Stages.")

        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                for idx, rec in enumerate(records, start=1):
                    try:
                        logging.debug(f"Processing call stage {idx}/{len(records)}: Id={rec.get('Id')}")

                        query = """
                            MERGE INTO dbo.SalesforceCallStages AS Target
                            USING (SELECT ? AS SalesforceId) AS Source
                            ON Target.SalesforceId = Source.SalesforceId
                            WHEN MATCHED THEN
                                UPDATE SET
                                    IsActive = ?,
                                    CallStage = ?,
                                    SortOrder = ?
                            WHEN NOT MATCHED THEN
                                INSERT (SalesforceId, IsActive, CallStage, SortOrder)
                                VALUES (?, ?, ?, ?);
                        """

                        values = [
                            rec.get('Id'),            # USING clause
                            rec.get('IsActive'),      # UPDATE
                            rec.get('MasterLabel'),   # UPDATE
                            rec.get('SortOrder'),     # UPDATE
                            rec.get('Id'),            # INSERT
                            rec.get('IsActive'),      # INSERT
                            rec.get('MasterLabel'),   # INSERT
                            rec.get('SortOrder')      # INSERT
                        ]

                        cursor.execute(query, values)

                    except pyodbc.DatabaseError as db_err:
                        logging.error(f"Database error while upserting call stage Id={rec.get('Id')}: {db_err}", exc_info=True)
                    except Exception as ex:
                        logging.error(f"Unexpected error while upserting call stage Id={rec.get('Id')}: {ex}", exc_info=True)

                conn.commit()
                logging.info("All call stages upserted successfully.")

        except pyodbc.InterfaceError as conn_err:
            logging.critical(f"Database connection failed: {conn_err}", exc_info=True)
        except Exception as ex:
            logging.critical(f"Unexpected error during call stages upsertion: {ex}", exc_info=True)
    def upsert_customField_crm_attributes(self, records):
        """
        Upserts records into dbo.SalesforceCrmAttributes.
        Each record must contain:
        - 'CustomFieldId' (SalesforceId)
        - 'CompanyId', 'Object', 'Name', 'Label', etc.
        """
        logging.info("Starting upsertion for Salesforce CRM Attributes.")

        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                for idx, rec in enumerate(records, start=1):
                    try:
                        logging.debug(f"Processing CRM attribute {idx}/{len(records)} - Id={rec.get('CustomFieldId')}")

                        query = """
                            MERGE dbo.SalesforceCrmAttributes AS target
                            USING (SELECT ? AS SalesforceId) AS source
                            ON (target.SalesforceId = source.SalesforceId)
                            WHEN MATCHED THEN
                                UPDATE SET
                                    CompanyId = ?,
                                    ObjectType = ?,
                                    Name = ?,
                                    Label = ?,
                                    Description = ?,
                                    Fieldtype = ?,
                                    DataType = ?,
                                    HasUniqueValue = ?,
                                    IsActive = ?,
                                    CreatedBy = ?,
                                    UpdatedBy = ?,
                                    CreatedAt = ?,
                                    UpdatedAt = ?
                            WHEN NOT MATCHED THEN
                                INSERT (
                                    SalesforceId,
                                    CompanyId,
                                    ObjectType,
                                    Name,
                                    Label,
                                    Description,
                                    Fieldtype,
                                    DataType,
                                    HasUniqueValue,
                                    IsActive,
                                    CreatedBy,
                                    UpdatedBy,
                                    CreatedAt,
                                    UpdatedAt
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """

                        values = (
                            # USING
                            rec.get('CustomFieldId'),

                            # UPDATE
                            rec.get('CompanyId'),
                            rec.get('Object'),
                            rec.get('Name'),
                            rec.get('Label'),
                            rec.get('Description'),
                            rec.get('FieldType'),
                            rec.get('DataType'),
                            rec.get('IsUnique'),
                            rec.get('IsActive'),
                            rec.get('CreatedBy'),
                            rec.get('UpdatedBy'),
                            self.safe_parse_date(rec.get('CreatedAt')),
                            self.safe_parse_date(rec.get('UpdatedAt')),

                            # INSERT
                            rec.get('CustomFieldId'),
                            rec.get('CompanyId'),
                            rec.get('Object'),
                            rec.get('Name'),
                            rec.get('Label'),
                            rec.get('Description'),
                            rec.get('FieldType'),
                            rec.get('DataType'),
                            rec.get('IsUnique'),
                            rec.get('IsActive'),
                            rec.get('CreatedBy'),
                            rec.get('UpdatedBy'),
                            self.safe_parse_date(rec.get('CreatedAt')),
                            self.safe_parse_date(rec.get('UpdatedAt')),
                        )

                        cursor.execute(query, values)

                    except pyodbc.DatabaseError as db_err:
                        logging.error(f"Database error on CRM attribute {rec.get('CustomFieldId')}: {db_err}", exc_info=True)
                    except Exception as ex:
                        logging.error(f"Unexpected error on CRM attribute {rec.get('CustomFieldId')}: {ex}", exc_info=True)

                conn.commit()
                logging.info("All Salesforce CRM attributes upserted successfully.")

        except pyodbc.InterfaceError as conn_err:
            logging.critical(f"Database connection failed: {conn_err}", exc_info=True)
        except Exception as ex:
            logging.critical(f"Unexpected failure in CRM attribute upsertion: {ex}", exc_info=True)


    def upsert_crm_attribute_values(self, records):
        """
        Upserts CRM attribute values into dbo.SalesforceCrmAttributeValues.
        Each record contains multiple FieldValues tied to a custom field.
        """
        logging.info("Starting upsertion into SalesforceCrmAttributeValues.")

        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                total_values = 0

                for rec in records:
                    object_type = rec.get("Object")
                    field_type = rec.get("DataType")
                    created_at = self.safe_parse_date(rec.get("CreatedAt"))
                    updated_at = self.safe_parse_date(rec.get("UpdatedAt"))
                    name = rec.get("Name")
                    company_id = rec.get("CompanyId")
                    field_values = rec.get("FieldValues", [])

                    if not isinstance(field_values, list):
                        logging.warning(f"FieldValues for '{name}' is not a list. Skipping.")
                        continue

                    for field_value in field_values:
                        try:
                            external_id = field_value.get("record_id")
                            value = field_value.get("value")

                            if not external_id:
                                logging.warning(f"Skipping field value for '{name}': Missing 'record_id'.")
                                continue

                            query = """
                                MERGE dbo.SalesforceCrmAttributeValues AS target
                                USING (
                                    SELECT 
                                        ? AS CompanyId,
                                        ? AS ObjectType,
                                        ? AS ExternalId,
                                        ? AS Type,
                                        ? AS Name
                                ) AS source
                                ON target.CompanyId = source.CompanyId AND
                                target.ObjectType = source.ObjectType AND
                                target.ExternalId = source.ExternalId AND
                                target.Type = source.Type AND
                                target.Name = source.Name
                                WHEN MATCHED THEN
                                    UPDATE SET
                                        Value = ?,
                                        UpdatedAt = ?
                                WHEN NOT MATCHED THEN
                                    INSERT (CompanyId, ObjectType, ExternalId, Type, Name, Value, CreatedAt, UpdatedAt)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                            """

                            values = (
                                company_id,
                                object_type,
                                external_id,
                                field_type,
                                name,
                                # UPDATE
                                value,
                                updated_at,
                                # INSERT
                                company_id,
                                object_type,
                                external_id,
                                field_type,
                                name,
                                value,
                                created_at,
                                updated_at
                            )

                            cursor.execute(query, values)
                            total_values += 1

                        except Exception as e:
                            logging.error(f"Failed to upsert field value for '{name}', record_id={external_id}: {e}", exc_info=True)

                conn.commit()
                logging.info(f"Successfully upserted {total_values} CRM attribute values.")

        except Exception as e:
            logging.critical("Database operation failed during CRM attribute value upsertion.", exc_info=True)


    # def insert_Access_token(self, records):
    #     logging.info("Starting insertion for Salesforce Token.")

    #     try:
    #         from database.connection_manager import get_connection
    #         from datetime import datetime, timedelta
            
    #         with get_connection() as conn:
    #             cursor = conn.cursor()

    #             for i, rec in enumerate(records, start=1):
    #                 try:
    #                     logging.info("Processing token record %d of %d", i, len(records))
                        
    #                     # Debug: Log the ExpireAt value before processing
    #                     expire_at_raw = rec.get('ExpireAt')
    #                     logging.info("Raw ExpireAt value: %s (type: %s)", expire_at_raw, type(expire_at_raw))
                        
    #                     # Parse the ExpireAt date
    #                     expire_at_parsed = self.safe_parse_date(expire_at_raw)
    #                     logging.info("Parsed ExpireAt value: %s", expire_at_parsed)
                        
    #                     # Ensure ExpireAt is not None
    #                     if expire_at_parsed is None:
    #                         expire_at_parsed = datetime.utcnow() + timedelta(hours=2)
    #                         logging.warning("ExpireAt was None, using fallback: %s", expire_at_parsed)
                        
    #                     # VALIDATE ALL UUID FIELDS
    #                     record_id = self.validate_uuid(rec.get('Id'))
    #                     user_id = self.validate_uuid(rec.get('UserId')) if rec.get('UserId') else None
    #                     company_id = self.validate_uuid(rec.get('CompanyId'))  # This was missing!
                        
    #                     # Debug: Log all UUID values
    #                     logging.info("UUID validation - Record ID: %s, User ID: %s, Company ID: %s", 
    #                             record_id, user_id, company_id)
                        
    #                     query = """
    #                         INSERT INTO ThirdPartyAccessTokens (
                                
    #                             ProviderName,
    #                             ServiceType,
    #                             AccessToken,
    #                             RefreshToken,
    #                             ExpireAt,
    #                             UserId,
    #                             CreatedBy,
    #                             CreatedAt,
    #                             Version,
    #                             ModifiedBy,
    #                             ModifiedAt,
    #                             IsDeleted,
    #                             CompanyId,
    #                             Metadata,
    #                             Status
    #                         )
    #                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    #                     """

    #                     values = (
    #                         # record_id,                      # Validated UUID
    #                         rec.get('ProviderName'),
    #                         rec.get('ServiceType'),
    #                         rec.get('AccessToken'),
    #                         rec.get('RefreshToken'),
    #                         expire_at_parsed,
    #                         user_id,                        # Validated UUID or None
    #                         rec.get('CreatedBy'),
    #                         self.safe_parse_date(rec.get('CreatedAt')) or datetime.utcnow(),
    #                         rec.get('Version'),
    #                         rec.get('ModifiedBy'),
    #                         self.safe_parse_date(rec.get('ModifiedAt')) or datetime.utcnow(),
    #                         rec.get('IsDeleted'),
    #                         company_id,                     # Validated UUID - THIS WAS THE ISSUE!
    #                         rec.get('Metadata'),
    #                         rec.get('Status'),
    #                     )

    #                     # Debug: Log the values being inserted with types
    #                     debug_values = []
    #                     uuid_positions = [0, 6, 13]  # Id, UserId, CompanyId positions
    #                     for idx, val in enumerate(values):
    #                         if idx in uuid_positions:
    #                             debug_values.append(f"{val} (UUID)")
    #                         elif isinstance(val, datetime):
    #                             debug_values.append(f"{val} (datetime)")
    #                         else:
    #                             debug_values.append(str(val))
                        
    #                     logging.info("Inserting values: %s", debug_values)
                        
    #                     cursor.execute(query, values)
    #                     logging.info("Successfully inserted token record %d", i)

    #                 except pyodbc.DatabaseError as db_err:
    #                     logging.error("Database error on token record %d (ID: %s): %s", 
    #                                 i, rec.get('Id', 'unknown'), str(db_err), exc_info=True)
    #                 except Exception as ex:
    #                     logging.error("Unexpected error on token record %d (ID: %s): %s", 
    #                                 i, rec.get('Id', 'unknown'), str(ex), exc_info=True)

    #             conn.commit()
    #             logging.info("All Salesforce tokens inserted successfully.")

    #     except pyodbc.InterfaceError as conn_err:
    #         logging.critical("Database connection failed: %s", str(conn_err), exc_info=True)
    #     except Exception as ex:
    #         logging.critical("Unexpected failure in token insertion: %s", str(ex), exc_info=True)


    # def validate_uuid(self, value):
    #     """
    #     Validates and formats UUID for SQL Server UNIQUEIDENTIFIER.
    #     Returns properly formatted UUID string or generates new one if invalid.
    #     """
    #     try:
    #         import uuid
    #         if not value:
    #             return str(uuid.uuid4())
            
    #         # Convert to string and clean up
    #         uuid_str = str(value).strip().upper()
            
    #         # Try to parse as UUID
    #         validated_uuid = uuid.UUID(uuid_str)
    #         return str(validated_uuid).upper()  # SQL Server likes uppercase UUIDs
            
    #     except (ValueError, TypeError) as e:
    #         logging.warning("Invalid UUID '%s', generating new one: %s", value, str(e))
    #         import uuid
    #         return str(uuid.uuid4()).upper()
    from typing import Dict, Any
    # def insert_token_data(self, processed_data: Dict[str, Any]) -> bool:
    #     """
    #     Insert token data into the ThirdpartyAccessToken table.

    #     Args:
    #         processed_data (Dict[str, Any]): Processed token data dictionary

    #     Returns:
    #         bool: True if insertion successful, False otherwise
    #     """
    #     import logging
    #     from database.connection_manager import get_connection

    #     logging.info("Starting token data insertion into ThirdpartyAccessToken.")

    #     try:
    #         with get_connection() as conn:
    #             cursor = conn.cursor()

    #             insert_query = """
    #                 INSERT INTO ThirdPartyAccessTokens (
    #                     Id, ProviderName, ServiceType, AccessToken, RefreshToken, 
    #                     ExpireAt, UserId, CreatedBy, CreatedAt, Version, 
    #                     ModifiedBy, ModifiedAt, IsDeleted, CompanyId, Metadata, Status
    #                 ) VALUES (
    #                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    #                 )
    #             """

    #             values = (
    #                 processed_data.get('Id'),
    #                 processed_data.get('ProviderName'),
    #                 processed_data.get('ServiceType'),
    #                 processed_data.get('AccessToken'),
    #                 processed_data.get('RefreshToken'),
    #                 processed_data.get('ExpireAt'),
    #                 processed_data.get('UserId'),
    #                 processed_data.get('CreatedBy'),
    #                 processed_data.get('CreatedAt'),
    #                 processed_data.get('Version'),
    #                 processed_data.get('ModifiedBy'),
    #                 processed_data.get('ModifiedAt'),
    #                 processed_data.get('IsDeleted'),
    #                 processed_data.get('CompanyId'),
    #                 processed_data.get('Metadata'),
    #                 processed_data.get('Status')
    #             )

    #             logging.debug(f"Inserting token data for ID: {processed_data.get('Id')}")
    #             cursor.execute(insert_query, values)
    #             conn.commit()

    #             logging.info(f"Token data inserted successfully with ID: {processed_data.get('Id')}")
    #             return True

    #     except pyodbc.DatabaseError as db_err:
    #         logging.error(f"Database error during token insert for ID {processed_data.get('Id')}: {db_err}", exc_info=True)
    #     except pyodbc.InterfaceError as conn_err:
    #         logging.critical(f"Database connection failed: {conn_err}", exc_info=True)
    #     except Exception as ex:
    #         logging.error(f"Unexpected error during token insert for ID {processed_data.get('Id')}: {ex}", exc_info=True)

    #     return False
    def upsert_access_token(self, processed_data: Dict[str, Any]) -> bool:
        """
        Upsert token data into ThirdPartyAccessTokens using SQL MERGE.

        Args:
            processed_data (Dict[str, Any]): Token record to insert or update

        Returns:
            bool: True if operation succeeds, False otherwise
        """
        import logging
        from database.connection_manager import get_connection

        logging.info("Starting upsert into ThirdPartyAccessTokens...")

        try:
            with get_connection() as conn:
                cursor = conn.cursor()

                query = """
                    MERGE dbo.ThirdPartyAccessTokens AS target
                    USING (SELECT ? AS Id) AS source
                    ON (target.Id = source.Id)
                    WHEN MATCHED THEN
                        UPDATE SET
                            AccessToken = ?,
                            RefreshToken = ?,
                            ExpireAt = ?,
                            ModifiedBy = ?,
                            ModifiedAt = ?,
                            Metadata = ?,
                            Status = ?
                    WHEN NOT MATCHED THEN
                        INSERT (
                            Id, ProviderName, ServiceType, AccessToken, RefreshToken,
                            ExpireAt, UserId, CreatedBy, CreatedAt, Version,
                            ModifiedBy, ModifiedAt, IsDeleted, CompanyId, Metadata, Status
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """

                values = (
                    # USING (Id)
                    processed_data.get('Id'),

                    # UPDATE SET
                    processed_data.get('AccessToken'),
                    processed_data.get('RefreshToken'),
                    processed_data.get('ExpireAt'),
                    processed_data.get('ModifiedBy'),
                    processed_data.get('ModifiedAt'),
                    processed_data.get('Metadata'),
                    processed_data.get('Status'),

                    # INSERT VALUES
                    processed_data.get('Id'),
                    processed_data.get('ProviderName'),
                    processed_data.get('ServiceType'),
                    processed_data.get('AccessToken'),
                    processed_data.get('RefreshToken'),
                    processed_data.get('ExpireAt'),
                    processed_data.get('UserId'),
                    processed_data.get('CreatedBy'),
                    processed_data.get('CreatedAt'),
                    processed_data.get('Version'),
                    processed_data.get('ModifiedBy'),
                    processed_data.get('ModifiedAt'),
                    processed_data.get('IsDeleted'),
                    processed_data.get('CompanyId'),
                    processed_data.get('Metadata'),
                    processed_data.get('Status'),
                )

                cursor.execute(query, values)
                conn.commit()
                logging.info(f"Upsert successful for token ID: {processed_data.get('Id')}")
                return True

        except Exception as ex:
            logging.error(f"Upsert failed for token ID {processed_data.get('Id')}: {ex}", exc_info=True)
            return False