import azure.functions as func
import logging
from routes.oauth_routes import handle_oauth_connect,handle_oauth_callback
import json,os
from routes.salesforce_functions import SalesForceExtraction
from datetime import datetime
from log_setup import azure_func_logs,simple_logger
logger = simple_logger()
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)



salesforce_data_extractor = SalesForceExtraction()
@app.route(route="salesforce_data_sync")
def salesforce_data_sync(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    
@app.route(route="oauth/connect", methods=["GET"])
def oauth_connect(req: func.HttpRequest) -> func.HttpResponse:
    """
    Redirect user to Salesforce authorization URL
    """
    logger.info('OAuth connect endpoint triggered')
    
    try:
        result = handle_oauth_connect(req)
        auth_url = result["auth_url"]
        
        # You should securely store code_verifier in session, cache, or DB mapped to user
        # Example (if needed): session["code_verifier"] = result["code_verifier"]

        return func.HttpResponse(
            status_code=302,
            headers={
                "Location": auth_url
            }
        )
    except Exception as e:
        logging.error(f"OAuth connect error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
@app.route(route="oauth/callback", methods=["GET", "POST"])
def oauth_callback(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle OAuth callback from Salesforce
    Exchange code for tokens and store in database
    """
    logger.info('OAuth callback endpoint triggered')
    
    try:
        result = handle_oauth_callback(req)
        return func.HttpResponse(
            json.dumps(result),
            status_code=200 if result.get("success") else 400,
            mimetype="application/json"
        )
    except Exception as e:
        logger.error(f"OAuth callback error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
http_logger, timer_logger = azure_func_logs()
@app.route(route="sync-salesforce", methods=["POST"])
def sync_Salesforce_tables(req: func.HttpRequest) -> func.HttpResponse:
    """Sync only opportunities data"""
    http_logger.info('Salesforce Data sync triggered')
    
    try:
        
        company_id = os.getenv('DEFAULT_COMPANY_ID', 'D6C8558D-DB96-458D-A7C3-865B688F629E')
        salesforce_data_extractor.sf_accounts(http_logger,company_id)
        salesforce_data_extractor.sf_opportunities(http_logger,company_id)
        salesforce_data_extractor.sf_contacts(http_logger,company_id)
        salesforce_data_extractor.sf_opportunity_activities(http_logger,company_id)
        salesforce_data_extractor.sf_opportunity_history(http_logger,company_id)
        salesforce_data_extractor.sf_users(http_logger,company_id)
        salesforce_data_extractor.sf_callstages(http_logger,company_id)
        salesforce_data_extractor.sf_custom_fields_full(http_logger,company_id)

        
        return func.HttpResponse(
            json.dumps({"success":True}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        http_logger.error(f"Salesforce opportunities sync error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
@app.route(route="test-error",methods=["GET"])
def test_error(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Received test-error request.")
    from Helper.HelperFunc import HelperFunc
    from database.connection_manager import get_connection
    from services.salesforce_client import execute_soql_query

    try:
        company_id = os.getenv('DEFAULT_COMPANY_ID', 'D6C8558D-DB96-458D-A7C3-865B688F629E')

        # Get Salesforce token from DB
        helper = HelperFunc(get_connection)
        token_data = helper.get_salesforce_token_from_db(company_id)

        access_token = token_data["AccessToken"]
        instance_url = token_data["Metadata"].get("instance_url", "")

        # Sample SOQL Query (replace this as needed)
        query = """
             SELECT
          Id, Name, Type, BillingStreet, BillingCity,
          BillingState, BillingPostalCode, BillingCountry, ShippingStreet, ShippingCity,
          ShippingState, ShippingPostalCode, ShippingCountry, Phone, Fax, 
          Website, PhotoUrl, Industry, AnnualRevenue,
          NumberOfEmployees, Description, OwnerId, 
          AccountSource, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById
        FROM Account
        """

        # Execute SOQL
        data = execute_soql_query(access_token,instance_url, query)

        return func.HttpResponse(str(data), status_code=200)

    except Exception as e:
        logging.exception("Unexpected error during SOQL execution.")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

@app.function_name(name="HourlySyncTimer")
@app.timer_trigger(
    schedule="0 0 * * * *",
    arg_name="timer",
    run_on_startup=False
)
def Periodicaly_Salesforce_Sync(timer: func.TimerRequest) -> None:
    timer_logger.info("Timer Salesforce Data sync triggered at %s", datetime.utcnow().isoformat())
    try:
        
        company_id = os.getenv('DEFAULT_COMPANY_ID', 'D6C8558D-DB96-458D-A7C3-865B688F629E')
        salesforce_data_extractor.sf_accounts(timer_logger,company_id)
        salesforce_data_extractor.sf_opportunities(timer_logger,company_id)
        salesforce_data_extractor.sf_contacts(timer_logger,company_id)
        salesforce_data_extractor.sf_opportunity_activities(timer_logger,company_id)
        salesforce_data_extractor.sf_opportunity_history(timer_logger,company_id)
        salesforce_data_extractor.sf_users(timer_logger,company_id)
        salesforce_data_extractor.sf_callstages(timer_logger,company_id)
        salesforce_data_extractor.sf_custom_fields_full(timer_logger,company_id)
        timer_logger.info("Salesforce sync completed successfully")
    except Exception as e:
        timer_logger.error(f"Salesforce sync error: {e}")

@app.route(route="salesforce/{object_type}", methods=["POST"])
def handle_salesforce_webhook(req: func.HttpRequest) -> func.HttpResponse:
    from webhook.route_webhook import handle_salesforce_webhook as logic
    object_type = req.route_params.get("object_type")
    return logic(req, object_type)
