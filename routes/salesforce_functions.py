from datetime import datetime,timedelta
from requests.exceptions import HTTPError
from routes.oauth_routes import refresh_access_token
from Helper.query import SalesforceQueries
from services.salesforce_client import execute_soql_query
from database.connection_manager import get_connection
from Helper.HelperFunc import HelperFunc
# from config import ACCESS_TOKEN,INSTANCE_URL
import pytz,json
from queries.insert_query import SalesforceUpsertFunctions
helper = HelperFunc(get_connection)

# Queries For Extracting data from Salesforce
sfq = SalesforceQueries()
# from config import INSTANCE_URL
# Insert functions for upserting data in DB
inserter = SalesforceUpsertFunctions()


class SalesForceExtraction:

    def __init__(self):
        self.helper = helper
        self.inserter = inserter
        self.sfq = sfq


    def sf_accounts(self,logger,company_id):
        try:
            logger.info(" Salesforce accounts sync started")
            last_sync = helper.get_last_sync_time("accounts")

            if last_sync and not isinstance(last_sync, datetime):
                raise TypeError(
                f"Expected datetime.datetime, got {type(last_sync)}"
                )

            # Build SOQL query
            if last_sync:
                formatted = last_sync.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                query = f"{sfq.base_query_accounts} WHERE LastModifiedDate > {formatted}"
            else:
                query = f"{sfq.base_query_accounts} ORDER BY LastModifiedDate ASC"

            logger.info("Executing SOQL query…")
        
            token_data = helper.get_salesforce_token_from_db(company_id)
            access_token = token_data['AccessToken']
            instance_url = token_data["Metadata"].get("instance_url", "")
            refresh_token = token_data["RefreshToken"]
            token_id = token_data["Id"]
            user_id = token_data["UserId"]
            try:
                data = execute_soql_query(
                access_token,
                instance_url=instance_url,
                soql=query)
            except HTTPError as http_err:
                
                if http_err.response.status_code == 401:
                    logger.warning("Access token expired. Attempting to refresh...")
                    refreshed = refresh_access_token(refresh_token)
                    if not refresh_token:
                        raise Exception("Token Refresh Failed")
                    updated_data = {
                        "Id": token_id,
                        "ProviderName": 3,
                        "ServiceType": 3,
                        "AccessToken": refreshed["access_token"],
                        "RefreshToken": refreshed.get("refresh_token", refresh_token),
                        "ExpireAt": datetime.utcnow() + timedelta(hours=4),
                        "UserId": user_id,
                        "CreatedBy": user_id,
                        "CreatedAt": datetime.utcnow(),
                        "Version": 1,
                        "ModifiedBy": user_id,
                        "ModifiedAt": datetime.utcnow(),
                        "IsDeleted": 0,
                        "CompanyId": company_id,
                        "Metadata": json.dumps({
                            "instance_url": refreshed.get("instance_url", instance_url),
                            "token_type": refreshed.get("token_type", ""),
                            "expires_in_seconds": refreshed.get("expires_in", ""),
                            "token_id": refreshed.get("id", "")
                        }),
                        "Status": 0
                    }
                    inserter.upsert_access_token(updated_data)
                    data = execute_soql_query(refreshed["access_token"],instance_url,query)
                else:
                    raise


            logger.info("Inserting accounts into DB…")
            if data:
                inserter.insert_account(data)
                helper.update_sync_log('accounts', datetime.now(pytz.UTC))

            logger.info("Accounts inserted, count: %d", len(data))
            return {
                "success": True,
                "sync_type": "incremental" if last_sync else "full",
                "records_processed": len(data),
                "last_sync": last_sync.isoformat() if last_sync else None,
                "status_code": 200
            }

        except TypeError as te:
            logger.error("Type error in sf_accounts: %s", te)
            return {
                "success": False,
                "error": "Invalid timestamp format",
                "details": str(te),
                "status_code": 400
            }

        except Exception as e:
            logger.error("Unexpected error in sf_accounts: %s", e)
            return {
                "success": False,
                "error": "Internal server error",
                "details": str(e),
                "status_code": 500
            }

    def sf_opportunities(self, logger, company_id):
        try:
            logger.info("Salesforce opportunities sync started")
            last_sync = helper.get_last_sync_time('opportunities')

            if last_sync and not isinstance(last_sync, datetime):
                raise TypeError(f"Expected datetime.datetime, got {type(last_sync)}")

            if last_sync:
                formatted = last_sync.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                query = f"{sfq.base_query_opportunities} WHERE LastModifiedDate > {formatted}"
            else:
                query = f"{sfq.base_query_opportunities} ORDER BY LastModifiedDate ASC"

            logger.info("Executing SOQL query for opportunities…")

            token_data = helper.get_salesforce_token_from_db(company_id)
            if not token_data:
                raise Exception("Salesforce token not found")

            access_token = token_data["AccessToken"]
            refresh_token = token_data.get("RefreshToken")
            instance_url = token_data["Metadata"].get("instance_url", "")
            token_id = token_data["Id"]
            user_id = token_data["UserId"]

            try:
                data = execute_soql_query(access_token, instance_url, query)

            except HTTPError as err:
                from routes.oauth_routes import refresh_access_token

                if err.response and err.response.status_code == 401:
                    logger.warning("Access token expired. Refreshing...")

                    refreshed = refresh_access_token(refresh_token)
                    if not refreshed or "access_token" not in refreshed:
                        raise Exception("Token refresh failed or invalid response")

                    expires_in = refreshed.get("expires_in", 14400)

                    updated_data = {
                        "Id": token_id,
                        "ProviderName": 3,
                        "ServiceType": 3,
                        "AccessToken": refreshed["access_token"],
                        "RefreshToken": refreshed.get("refresh_token", refresh_token),
                        "ExpireAt": datetime.utcnow() + timedelta(seconds=int(expires_in)),
                        "UserId": user_id,
                        "CreatedBy": user_id,
                        "CreatedAt": datetime.utcnow(),
                        "Version": 1,
                        "ModifiedBy": user_id,
                        "ModifiedAt": datetime.utcnow(),
                        "IsDeleted": 0,
                        "CompanyId": company_id,
                        "Metadata": json.dumps({
                            "instance_url": refreshed.get("instance_url", instance_url),
                            "token_type": refreshed.get("token_type", ""),
                            "expires_in_seconds": refreshed.get("expires_in", ""),
                            "token_id": refreshed.get("id", "")
                        }),
                        "Status": 0
                    }

                    inserter.upsert_access_token(updated_data)

                    data = execute_soql_query(refreshed["access_token"],
                                            updated_data["Metadata"]["instance_url"],
                                            query)
                else:
                    raise

            logger.info("Inserting/upserting into opportunities table…")
            if data:
                inserter.upsert_opportunity(data)
                helper.update_sync_log('opportunities', datetime.now(pytz.UTC))

            logger.info("Opportunities sync completed, count: %d", len(data))
            return {
                "success": True,
                "sync_type": "incremental" if last_sync else "full",
                "records_processed": len(data),
                "last_sync": last_sync.isoformat() if last_sync else None,
                "status_code": 200
            }

        except TypeError as te:
            logger.error("Type error in sf_opportunities: %s", te)
            return {
                "success": False,
                "error": "Invalid timestamp format",
                "details": str(te),
                "status_code": 400
            }

        except Exception as e:
            logger.error("Unexpected error in sf_opportunities: %s", e, exc_info=True)
            return {
                "success": False,
                "error": "Internal server error",
                "details": str(e),
                "status_code": 500
            }

    def sf_contacts(self, logger, company_id):
        try:
            logger.info("Salesforce contacts sync started")
            last_sync = helper.get_last_sync_time('contacts')

            if last_sync and not isinstance(last_sync, datetime):
                raise TypeError(f"Expected datetime.datetime, got {type(last_sync)}")

            if last_sync:
                formatted = last_sync.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                query = f"{sfq.base_query_contacts} WHERE LastModifiedDate > {formatted}"
            else:
                query = f"{sfq.base_query_contacts} ORDER BY LastModifiedDate ASC"

            logger.info("Executing SOQL query for contacts…")

            token_data = helper.get_salesforce_token_from_db(company_id)
            if not token_data:
                raise Exception("Salesforce token not found")

            access_token = token_data["AccessToken"]
            refresh_token = token_data.get("RefreshToken")
            instance_url = token_data["Metadata"].get("instance_url", "")
            token_id = token_data["Id"]
            user_id = token_data["UserId"]

            try:
                data = execute_soql_query(access_token, instance_url, query)

            except HTTPError as err:
                from routes.oauth_routes import refresh_access_token

                if err.response and err.response.status_code == 401:
                    logger.warning("Access token expired. Refreshing...")

                    refreshed = refresh_access_token(refresh_token)
                    if not refreshed or "access_token" not in refreshed:
                        raise Exception("Token refresh failed or invalid response")

                    expires_in = refreshed.get("expires_in", 14400)

                    updated_data = {
                        "Id": token_id,
                        "ProviderName": 3,
                        "ServiceType": 3,
                        "AccessToken": refreshed["access_token"],
                        "RefreshToken": refreshed.get("refresh_token", refresh_token),
                        "ExpireAt": datetime.utcnow() + timedelta(seconds=int(expires_in)),
                        "UserId": user_id,
                        "CreatedBy": user_id,
                        "CreatedAt": datetime.utcnow(),
                        "Version": 1,
                        "ModifiedBy": user_id,
                        "ModifiedAt": datetime.utcnow(),
                        "IsDeleted": 0,
                        "CompanyId": company_id,
                        "Metadata": json.dumps({
                            "instance_url": refreshed.get("instance_url", instance_url),
                            "token_type": refreshed.get("token_type", ""),
                            "expires_in_seconds": refreshed.get("expires_in", ""),
                            "token_id": refreshed.get("id", "")
                        }),
                        "Status": 0
                    }

                    inserter.upsert_access_token(updated_data)

                    data = execute_soql_query(
                        refreshed["access_token"],
                        updated_data["Metadata"].get("instance_url", instance_url),
                        query
                    )
                else:
                    raise

            logger.info("Inserting/upserting into contacts table…")
            if data:
                inserter.upsert_contact(data)
                helper.update_sync_log('contacts', datetime.now(pytz.UTC))

            logger.info("Contacts sync completed, count: %d", len(data))
            return {
                "success": True,
                "sync_type": "incremental" if last_sync else "full",
                "records_processed": len(data),
                "last_sync": last_sync.isoformat() if last_sync else None,
                "data": data,
                "status_code": 200
            }

        except TypeError as te:
            logger.error(f"Type error in sf_contacts: {te}")
            return {
                "success": False,
                "error": "Invalid timestamp format",
                "details": str(te),
                "status_code": 400
            }

        except Exception as e:
            logger.error(f"Unexpected error in sf_contacts: {e}", exc_info=True)
            return {
                "success": False,
                "error": "Internal server error",
                "details": str(e),
                "status_code": 500
            }

    def sf_opportunity_activities(self, logger, company_id):
        try:
            logger.info("Salesforce opportunity activities sync started")
            last_sync = helper.get_last_sync_time('opportunity_activities')

            if last_sync and not isinstance(last_sync, datetime):
                raise TypeError(f"Expected datetime.datetime, got {type(last_sync)}")

            if last_sync:
                formatted = last_sync.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                query = f"{sfq.base_query_opp_Activities} AND LastModifiedDate > {formatted}"
            else:
                query = f"{sfq.base_query_opp_Activities} ORDER BY LastModifiedDate ASC"

            logger.info("Executing SOQL query for opportunity activities…")

            token_data = helper.get_salesforce_token_from_db(company_id)
            if not token_data:
                raise Exception("Salesforce token not found")

            access_token = token_data["AccessToken"]
            refresh_token = token_data.get("RefreshToken")
            instance_url = token_data["Metadata"].get("instance_url", "")
            token_id = token_data["Id"]
            user_id = token_data["UserId"]

            try:
                data = execute_soql_query(access_token, instance_url, query)

            except HTTPError as err:
                from routes.oauth_routes import refresh_access_token

                if err.response and err.response.status_code == 401:
                    logger.warning("Access token expired. Refreshing...")

                    refreshed = refresh_access_token(refresh_token)
                    if not refreshed or "access_token" not in refreshed:
                        raise Exception("Token refresh failed or invalid response")

                    expires_in = refreshed.get("expires_in", 14400)
                    updated_data = {
                        "Id": token_id,
                        "ProviderName": 3,
                        "ServiceType": 3,
                        "AccessToken": refreshed["access_token"],
                        "RefreshToken": refreshed.get("refresh_token", refresh_token),
                        "ExpireAt": datetime.utcnow() + timedelta(seconds=int(expires_in)),
                        "UserId": user_id,
                        "CreatedBy": user_id,
                        "CreatedAt": datetime.utcnow(),
                        "Version": 1,
                        "ModifiedBy": user_id,
                        "ModifiedAt": datetime.utcnow(),
                        "IsDeleted": 0,
                        "CompanyId": company_id,
                        "Metadata": json.dumps({
                            "instance_url": refreshed.get("instance_url", instance_url),
                            "token_type": refreshed.get("token_type", ""),
                            "expires_in_seconds": refreshed.get("expires_in", ""),
                            "token_id": refreshed.get("id", "")
                        }),
                        "Status": 0
                    }

                    inserter.upsert_access_token(updated_data)

                    data = execute_soql_query(
                        refreshed["access_token"],
                        updated_data["Metadata"].get("instance_url", instance_url),
                        query
                    )
                else:
                    raise

            logger.info("Inserting/upserting opportunity activities into DB")
            if data:
                inserter.upsert_opportunity_activities(data)
                helper.update_sync_log('opportunity_activities', datetime.now(pytz.UTC))

            logger.info("Opportunity activities sync completed, count: %d", len(data))
            return {
                "success": True,
                "sync_type": "incremental" if last_sync else "full",
                "records_processed": len(data),
                "last_sync": last_sync.isoformat() if last_sync else None,
                "data": data,
                "status_code": 200
            }

        except TypeError as te:
            logger.error(f"Type error in sf_opportunity_activities: {te}")
            return {
                "success": False,
                "error": "Invalid timestamp format",
                "details": str(te),
                "status_code": 400
            }

        except Exception as e:
            logger.error(f"Unexpected error in sf_opportunity_activities: {e}", exc_info=True)
            return {
                "success": False,
                "error": "Internal server error",
                "details": str(e),
                "status_code": 500
            }
    def sf_opportunity_history(self, logger, company_id):
        try:
            logger.info("Salesforce opportunity history sync started")
            last_sync = helper.get_last_sync_time('opportunity_history')

            if last_sync and not isinstance(last_sync, datetime):
                raise TypeError(f"Expected datetime.datetime, got {type(last_sync)}")

            if last_sync:
                formatted = last_sync.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                query = f"{sfq.base_query_opp_history} WHERE CreatedDate > {formatted}"
            else:
                query = f"{sfq.base_query_opp_history} ORDER BY CreatedDate ASC"

            logger.info("Executing SOQL query for opportunity history…")

            token_data = helper.get_salesforce_token_from_db(company_id)
            if not token_data:
                raise Exception("Salesforce token not found")

            access_token = token_data["AccessToken"]
            refresh_token = token_data.get("RefreshToken")
            instance_url = token_data["Metadata"].get("instance_url", "")
            token_id = token_data["Id"]
            user_id = token_data["UserId"]

            try:
                data = execute_soql_query(access_token, instance_url, query)

            except HTTPError as err:
                from routes.oauth_routes import refresh_access_token

                if err.response and err.response.status_code == 401:
                    logger.warning("Access token expired. Refreshing...")

                    refreshed = refresh_access_token(refresh_token)
                    if not refreshed or "access_token" not in refreshed:
                        raise Exception("Token refresh failed or invalid response")

                    expires_in = refreshed.get("expires_in", 14400)
                    updated_data = {
                        "Id": token_id,
                        "ProviderName": 3,
                        "ServiceType": 3,
                        "AccessToken": refreshed["access_token"],
                        "RefreshToken": refreshed.get("refresh_token", refresh_token),
                        "ExpireAt": datetime.utcnow() + timedelta(seconds=int(expires_in)),
                        "UserId": user_id,
                        "CreatedBy": user_id,
                        "CreatedAt": datetime.utcnow(),
                        "Version": 1,
                        "ModifiedBy": user_id,
                        "ModifiedAt": datetime.utcnow(),
                        "IsDeleted": 0,
                        "CompanyId": company_id,
                        "Metadata": json.dumps({
                            "instance_url": refreshed.get("instance_url", instance_url),
                            "token_type": refreshed.get("token_type", ""),
                            "expires_in_seconds": refreshed.get("expires_in", ""),
                            "token_id": refreshed.get("id", "")
                        }),
                        "Status": 0
                    }

                    inserter.upsert_access_token(updated_data)

                    data = execute_soql_query(
                        refreshed["access_token"],
                        updated_data["Metadata"].get("instance_url", instance_url),
                        query
                    )
                else:
                    raise

            logger.info("Inserting/upserting opportunity history into DB")
            if data:
                inserter.upsert_opp_history(data)
                helper.update_sync_log('opportunity_history', datetime.now(pytz.UTC))

            logger.info("Opportunity history sync completed, count: %d", len(data))
            return {
                "success": True,
                "sync_type": "incremental" if last_sync else "full",
                "records_processed": len(data),
                "last_sync": last_sync.isoformat() if last_sync else None,
                "data": data,
                "status_code": 200
            }

        except TypeError as te:
            logger.error(f"Type error in sf_opportunity_history: {te}")
            return {
                "success": False,
                "error": "Invalid timestamp format",
                "details": str(te),
                "status_code": 400
            }

        except Exception as e:
            logger.error(f"Unexpected error in sf_opportunity_history: {e}", exc_info=True)
            return {
                "success": False,
                "error": "Internal server error",
                "details": str(e),
                "status_code": 500
            }

    def sf_users(self, logger, company_id):
        try:
            logger.info("Salesforce users sync started")
            last_sync = helper.get_last_sync_time('users')

            if last_sync and not isinstance(last_sync, datetime):
                raise TypeError(f"Expected datetime, got {type(last_sync)}")

            if last_sync:
                formatted = last_sync.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                query = f"{sfq.base_query_users} WHERE LastModifiedDate > {formatted}"
            else:
                query = f"{sfq.base_query_users} ORDER BY LastModifiedDate ASC"

            logger.info("Executing SOQL query for users...")

            token_data = helper.get_salesforce_token_from_db(company_id)
            if not token_data:
                raise Exception("Salesforce token not found")

            access_token = token_data["AccessToken"]
            refresh_token = token_data.get("RefreshToken")
            instance_url = token_data["Metadata"].get("instance_url", "")
            token_id = token_data["Id"]
            user_id = token_data["UserId"]

            try:
                data = execute_soql_query(access_token, instance_url, query)

            except HTTPError as err:
                from routes.oauth_routes import refresh_access_token

                if err.response and err.response.status_code == 401:
                    logger.warning("Access token expired. Refreshing...")

                    refreshed = refresh_access_token(refresh_token)
                    if not refreshed or "access_token" not in refreshed:
                        raise Exception("Token refresh failed or invalid response")

                    expires_in = refreshed.get("expires_in", 14400)
                    updated_data = {
                        "Id": token_id,
                        "ProviderName": token_data.get("ProviderName", 3),
                        "ServiceType": token_data.get("ServiceType", 3),
                        "AccessToken": refreshed["access_token"],
                        "RefreshToken": refreshed.get("refresh_token", refresh_token),
                        "ExpireAt": datetime.utcnow() + timedelta(seconds=int(expires_in)),
                        "UserId": user_id,
                        "CreatedBy": user_id,
                        "CreatedAt": datetime.utcnow(),
                        "Version": token_data.get("Version", 1),
                        "ModifiedBy": user_id,
                        "ModifiedAt": datetime.utcnow(),
                        "IsDeleted": 0,
                        "CompanyId": company_id,
                        "Metadata": json.dumps({
                            "instance_url": refreshed.get("instance_url", instance_url),
                            "token_type": refreshed.get("token_type", ""),
                            "expires_in_seconds": refreshed.get("expires_in", ""),
                            "token_id": refreshed.get("id", "")
                        }),
                        "Status": 0
                    }

                    inserter.upsert_access_token(updated_data)

                    data = execute_soql_query(
                        refreshed["access_token"],
                        json.loads(updated_data["Metadata"])["instance_url"],
                        query
                    )
                else:
                    raise

            if data:
                logger.info("Inserting/updating users into DB")
                inserter.upsert_user(data)
                helper.update_sync_log('users', datetime.now(pytz.UTC))

            logger.info("Users sync completed. Count: %d", len(data))
            return {
                "success": True,
                "sync_type": "incremental" if last_sync else "full",
                "records_processed": len(data),
                "last_sync": last_sync.isoformat() if last_sync else None,
                "data": data,
                "status_code": 200
            }

        except TypeError as te:
            logger.error(f"Type error in sf_users: {te}")
            return {
                "success": False,
                "error": "Invalid timestamp format",
                "details": str(te),
                "status_code": 400
            }

        except Exception as e:
            logger.error(f"Unexpected error in sf_users: {e}", exc_info=True)
            return {
                "success": False,
                "error": "Internal server error",
                "details": str(e),
                "status_code": 500
            }


    def sf_callstages(self, logger, company_id):
        try:
            logger.info("Salesforce call stages sync started")
            last_sync = helper.get_last_sync_time('callstages')

            if last_sync and not isinstance(last_sync, datetime):
                raise TypeError(f"Expected datetime.datetime, got {type(last_sync)}")

            if last_sync:
                formatted = last_sync.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                query = f"{sfq.query_for_callstage} WHERE LastModifiedDate > {formatted}"
            else:
                query = f"{sfq.query_for_callstage} ORDER BY LastModifiedDate ASC"

            logger.info("Executing SOQL query for call stages…")

            token_data = helper.get_salesforce_token_from_db(company_id)
            if not token_data:
                raise Exception("Salesforce token not found")

            access_token = token_data["AccessToken"]
            refresh_token = token_data.get("RefreshToken")
            instance_url = token_data["Metadata"].get("instance_url", "")
            token_id = token_data["Id"]
            user_id = token_data["UserId"]

            try:
                data = execute_soql_query(access_token, instance_url, query)

            except HTTPError as err:
                from routes.oauth_routes import refresh_access_token

                if err.response and err.response.status_code == 401:
                    logger.warning("Access token expired. Refreshing...")

                    refreshed = refresh_access_token(refresh_token)
                    if not refreshed or "access_token" not in refreshed:
                        raise Exception("Token refresh failed or invalid response")

                    expires_in = refreshed.get("expires_in", 14400)
                    updated_data = {
                        "Id": token_id,
                        "ProviderName": token_data.get("ProviderName", 3),
                        "ServiceType": token_data.get("ServiceType", 3),
                        "AccessToken": refreshed["access_token"],
                        "RefreshToken": refreshed.get("refresh_token", refresh_token),
                        "ExpireAt": datetime.utcnow() + timedelta(seconds=int(expires_in)),
                        "UserId": user_id,
                        "CreatedBy": user_id,
                        "CreatedAt": datetime.utcnow(),
                        "Version": token_data.get("Version", 1),
                        "ModifiedBy": user_id,
                        "ModifiedAt": datetime.utcnow(),
                        "IsDeleted": 0,
                        "CompanyId": company_id,
                        "Metadata": json.dumps({
                            "instance_url": refreshed.get("instance_url", instance_url),
                            "token_type": refreshed.get("token_type", ""),
                            "expires_in_seconds": refreshed.get("expires_in", ""),
                            "token_id": refreshed.get("id", "")
                        }),
                        "Status": 0
                    }

                    inserter.upsert_access_token(updated_data)

                    data = execute_soql_query(
                        refreshed["access_token"],
                        json.loads(updated_data["Metadata"])["instance_url"],
                        query
                    )
                else:
                    raise

            if data:
                logger.info("Inserting/updating call stages into DB")
                inserter.upsert_call_stages(data)
                helper.update_sync_log('callstages', datetime.now(pytz.UTC))

            logger.info("Call stages sync completed. Count: %d", len(data))
            return {
                "success": True,
                "sync_type": "incremental" if last_sync else "full",
                "records_processed": len(data),
                "last_sync": last_sync.isoformat() if last_sync else None,
                "data": data,
                "status_code": 200
            }

        except TypeError as te:
            logger.error(f"Type error in sf_callstages: {te}")
            return {
                "success": False,
                "error": "Invalid timestamp format",
                "details": str(te),
                "status_code": 400
            }

        except Exception as e:
            logger.error(f"Unexpected error in sf_callstages: {e}", exc_info=True)
            return {
                "success": False,
                "error": "Internal server error",
                "details": str(e),
                "status_code": 500
            }

    
    def sf_custom_fields_full(self,logger,company_id):
        try:
            custom_field_tables = ['Account', 'Contact', 'Opportunity', 'User']
            all_data = []
            sync_results = {}

            for table in custom_field_tables:
                logger.info(f"Processing {table}...")

                table_sync_key = f"CrmAttributeValues_{table}"
                last_sync = helper.get_last_sync_time(table_sync_key)

                table_data = helper.sf_custom_fields_for_object(table,company_id, last_sync)

                for item in table_data:
                    item['Object'] = table

                sync_results[table] = {
                    "sync_type": "incremental" if last_sync else "full",
                    "records_processed": len(table_data),
                    "last_sync": last_sync.isoformat() if last_sync else None
                }

                if table_data:
                    logger.info(f"Found {len(table_data)} records for {table}")
                    all_data.extend(table_data)

                    inserter.upsert_customField_crm_attributes(table_data)
                    logger.info(f"Custom field metadata for {table} inserted")

                    inserter.upsert_crm_attribute_values(table_data)
                    logger.info(f"Custom field values for {table} inserted")

                    helper.update_sync_log(table_sync_key, datetime.now(pytz.UTC))
                    logger.info(f"Sync log updated for {table}")
                else:
                    if last_sync:
                        helper.update_sync_log(table_sync_key, datetime.now(pytz.UTC))
                        logger.info(f"No changes for {table}, but sync log updated")
                    else:
                        logger.info(f"No custom fields found for {table} (first sync)")

            return {
                "success": True,
                "total_records_processed": len(all_data),
                "sync_results": sync_results,
                "data": all_data,
                "status_code": 200
            }

        except Exception as e:
            logger.error(f"Unexpected error in sf_custom_fields_full: {str(e)}", exc_info=True)
            status_code = getattr(e, 'response', None).status_code if hasattr(e, 'response') else 500
            return {
                "success": False,
                "message": str(e),
                "status_code": status_code
            }


