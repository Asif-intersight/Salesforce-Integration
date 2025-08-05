# Add this to your existing Flask blueprint file
from webhook.webhook_handler import SalesforceWebhookHandler
from flask import request, jsonify,Blueprint
from routes.salesforce_functions import helper,inserter
from datetime import datetime
import logging
import pytz
import azure.functions as func
import json
from log_setup import webhook_logger
logger = webhook_logger()
# Initialize the webhook handler (assuming you have inserter and helper available)

webhook_handler = SalesforceWebhookHandler(inserter, helper)

def handle_salesforce_webhook(req: func.HttpRequest, object_type: str) -> func.HttpResponse:
    try:
        logger.info("Checking webhook log")
        logging.info(f"Received webhook for {object_type}")
        content_type = req.headers.get("Content-Type", "")
        logging.info(f"Content-Type: {content_type}")
        logging.info(f"Headers: {dict(req.headers)}")

        # Optional: Signature validation
        webhook_secret = None  # Load from config if needed
        if not webhook_handler.validate_webhook_signature(req, webhook_secret):
            logging.warning("Webhook signature validation failed")
            logger.warning("Webhook signature validation failed")
            return func.HttpResponse("Unauthorized", status_code=401)

        is_soap_request = 'xml' in content_type.lower() or 'soap' in content_type.lower()

        if is_soap_request:
            body = req.get_body()
            if not body:
                logging.warning("Empty SOAP payload received")
                logger.warning("Empty SOAP payload received")
                soap_response = webhook_handler.create_soap_acknowledgment(False)
                return func.HttpResponse(soap_response, status_code=400, mimetype="text/xml")

            logging.info(f"Raw SOAP data: {body.decode('utf-8', errors='ignore')}")
            payload = webhook_handler.parse_soap_request(body)
            if not payload:
                logging.error("Failed to parse SOAP payload")
                logger.error("Failed to parse SOAP payload")
                soap_response = webhook_handler.create_soap_acknowledgment(False)
                return func.HttpResponse(soap_response, status_code=400, mimetype="text/xml")

        else:
            try:
                payload = req.get_json()
                if not payload:
                    return func.HttpResponse(
                        json.dumps({"error": "Invalid JSON payload"}),
                        status_code=400,
                        mimetype="application/json"
                    )
            except Exception as e:
                return func.HttpResponse(
                    json.dumps({"error": f"JSON parsing error: {str(e)}"}),
                    status_code=400,
                    mimetype="application/json"
                )

        # 🔀 Object handler routing
        handlers = {
            'accounts': webhook_handler.handle_account_change,
            'opportunities': webhook_handler.handle_opportunity_change,
            'contacts': webhook_handler.handle_contact_change,
            'opportunity_activities': webhook_handler.handle_opportunity_activity_change,
            'users': webhook_handler.handle_users_change
        }

        handler = handlers.get(object_type.lower())
        if not handler:
            if is_soap_request:
                soap_response = webhook_handler.create_soap_acknowledgment(False)
                return func.HttpResponse(soap_response, status_code=400, mimetype="text/xml")
            else:
                return func.HttpResponse(
                    json.dumps({
                        "error": f"Unsupported object type: {object_type}",
                        "supported_types": list(handlers.keys())
                    }),
                    status_code=400,
                    mimetype="application/json"
                )

        # ✅ Call object-specific handler
        logging.info(f"Processing {object_type} with payload: {payload}")
        result = handler(payload)
        logging.info(f"Handler result: {result}")

        # 🧾 Response construction
        if is_soap_request:
            success = result.get("status") == "success"
            soap_response = webhook_handler.create_soap_acknowledgment(success)
            return func.HttpResponse(soap_response, status_code=200, mimetype="text/xml")
        else:
            response_json = {
                "success": result.get("status") == "success",
                "message": f"{object_type} processed successfully",
                "records_processed": result.get("count", 0),
                "timestamp": datetime.now(pytz.UTC).isoformat(),
                "error": result.get("error") if result.get("status") == "error" else None
            }
            return func.HttpResponse(
                json.dumps(response_json),
                status_code=200,
                mimetype="application/json"
            )

    except Exception as e:
        logging.exception(f"Webhook error for {object_type}: {e}")
        logger.exception(f"Webhook error for {object_type}: {e}")
        if 'xml' in content_type.lower() or 'soap' in content_type.lower():
            soap_response = webhook_handler.create_soap_acknowledgment(False)
            return func.HttpResponse(soap_response, status_code=500, mimetype="text/xml")
        else:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.now(pytz.UTC).isoformat()
                }),
                status_code=500,
                mimetype="application/json"
            )