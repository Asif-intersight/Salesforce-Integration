# from flask import Blueprint, redirect, request, session, jsonify
import os, base64, hashlib, time, logging,requests
from config import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, AUTHORIZE_URL, TOKEN_URL
from queries.insert_query import SalesforceUpsertFunctions
import uuid
from urllib.parse import urlencode, parse_qs
from datetime import datetime,timedelta
import json
from typing import Dict,Any,Union
import azure.functions as func
# oauth_bp = Blueprint('oauth', __name__)
from flask import session

from log_setup import simple_logger

logger = simple_logger()

inserter = SalesforceUpsertFunctions()

def generate_pkce_pair():
    verifier = base64.urlsafe_b64encode(os.urandom(40)).rstrip(b'=').decode()
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b'=').decode()
    return verifier, challenge

def handle_oauth_connect(req: func.HttpRequest):
    """
    Handle OAuth connection initiation
    Encode code_verifier in the state parameter (Base64 encoded)
    """
    try:
        verifier, challenge = generate_pkce_pair()
        
        # Encode verifier in state parameter (simple Base64 encoding)
        # In production, you might want to encrypt this
        encoded_verifier = base64.urlsafe_b64encode(verifier.encode()).decode()
        state = f"cv_{encoded_verifier}"
        
        # Build authorization URL with state parameter containing verifier
        auth_params = {
            'response_type': 'code',
            'client_id': CLIENT_ID,
            'redirect_uri': REDIRECT_URI,
            'scope': 'api refresh_token',
            'code_challenge': challenge,
            'code_challenge_method': 'S256',
            'state': state  # Contains encoded verifier
        }
        
        auth_url = f"{AUTHORIZE_URL}?{urlencode(auth_params)}"
        
        logging.info("Generated Salesforce authorization URL with encoded verifier in state")
        logger.info("Generated Salesforce authorization URL with encoded verifier in state")
        
        return {
            "success": True,
            "auth_url": auth_url,
            "message": "Redirect user to auth_url. Verifier is encoded in state parameter."
        }
        
    except Exception as e:
        logging.error(f"Failed to initiate OAuth flow: {e}")
        logger.error(f"Failed to initiate OAuth flow: {e}")
        return {
            "success": False, 
            "error": "OAuth connection failed", 
            "details": str(e)
        }
def handle_oauth_callback(req: func.HttpRequest):
    """
    Handle OAuth callback from Salesforce
    Decode code_verifier from state parameter
    """
    try:
        # Get parameters from query string (Salesforce redirects with GET)
        code = req.params.get('code')
        state = req.params.get('state')
        error = req.params.get('error')
        
        # Check for OAuth errors
        if error:
            logging.error(f"OAuth error from Salesforce: {error}")
            logger.error(f"OAuth error from Salesforce: {error}")
            return {
                "success": False,
                "error": f"Salesforce OAuth error: {error}"
            }
        
        if not code or not state:
            logging.warning(f"Missing code or state in callback. Code: {bool(code)}, State: {bool(state)}")
            logger.warning(f"Missing code or state in callback. Code: {bool(code)}, State: {bool(state)}")
            return {
                "success": False, 
                "error": "Missing code or state parameter"
            }
        
        # Decode code_verifier from state parameter
        try:
            if not state.startswith('cv_'):
                raise ValueError("Invalid state format")
            
            encoded_verifier = state[3:]  # Remove 'cv_' prefix
            code_verifier = base64.urlsafe_b64decode(encoded_verifier.encode()).decode()
            
        except Exception as e:
            logging.error(f"Failed to decode verifier from state: {e}")
            logger.error(f"Failed to decode verifier from state: {e}")
            return {
                "success": False,
                "error": "Invalid state parameter"
            }
        
        # Exchange code for tokens
        token_data = exchange_code_for_tokens(code, code_verifier)
        if not token_data:
            return {
                "success": False,
                "error": "Failed to exchange code for tokens"
            }
        
        # Get company_id
        company_id = os.getenv('DEFAULT_COMPANY_ID', 'D6C8558D-DB96-458D-A7C3-865B688F629E')
        user_id = "0194400E-BFF7-7062-A535-2C9AD4D17713"
        # Store tokens in database
        # logging.info(token_data)
        logging.info("------------------------------------------------------------------------------")
        logging.info(token_data)
        logging.info("------------------------------------------------------------------------------")
        result = store_tokens_in_db(token_data, company_id,user_id)
        
        logging.info("OAuth flow completed successfully")
        return result
        
    except Exception as e:
        logging.error(f"OAuth callback error: {e}")
        return {
            "success": False,
            "error": "OAuth callback failed",
            "details": str(e)
        }

def exchange_code_for_tokens(code, code_verifier):
    """Exchange authorization code for access and refresh tokens"""
    try:
        token_payload = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code_verifier": code_verifier
        }
        
        response = requests.post(TOKEN_URL, data=token_payload)
        response.raise_for_status()
        
        token_data = response.json()
        logging.info("Successfully exchanged code for tokens")
        
        return token_data
        
    except requests.RequestException as e:
        logging.error(f"Token exchange failed: {e}")
        if hasattr(e, 'response') and e.response:
            logging.error(f"Response content: {e.response.text}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during token exchange: {e}")
        return None
def store_tokens_in_db(token_response, company_id: str, user_id: str) -> Dict[str, Any]:
    """
    Processes token data and prepares it for database insertion.

    Args:
        token_response (str | dict): JSON string or dict containing token data
        company_id (str): Company ID (GUID)
        user_id (str): User ID (GUID)

    Returns:
        Dict[str, Any]: Status of operation
    """
    try:
        token_info = token_response

        record_id = str(uuid.uuid4()).upper()
        # current_time = datetime.now().isoformat() + " +00:00"

        access_token = token_info['access_token']
        refresh_token = token_info['refresh_token']

        processed_data = {
            'Id': record_id,
            'ProviderName': 3,
            'ServiceType': 3,
            'AccessToken': access_token,
            'RefreshToken': refresh_token,
            'ExpireAt': datetime.utcnow() + timedelta(hours=4),
            'UserId': user_id,
            'CreatedBy': user_id,
            'CreatedAt': datetime.utcnow(),
            'Version': 1,
            'ModifiedBy': user_id,
            'ModifiedAt': datetime.utcnow(),
            'IsDeleted': 0,
            'CompanyId': company_id,
            'Metadata': json.dumps({
                "instance_url": token_info.get('instance_url', ''),
                "token_type": token_info.get('token_type', ''),
                "expires_in_seconds": token_info.get('expires_in_seconds', ''),
                "token_id": token_info.get('token_id', '')
            }),
            'Status': 0
        }

        inserter.upsert_access_token(processed_data)
        return {
            "success": True,
            "message": "Token data inserted",
            "access_token":access_token,
            "refresh_token":refresh_token,
            "expireat":token_info.get('expires_at', '')
        }

    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")
    except Exception as e:
        raise Exception(f"Error processing token data: {e}")


def refresh_access_token(refresh_token):
    """Refresh access token using refresh token"""
    logger.info("------------------------inside refresh token-----------------------------")
    try:
        refresh_payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        }
        logger.info("refreshing token")
        
        response = requests.post(TOKEN_URL, data=refresh_payload)
        logger.info("-------------------after refresh token----------")
        logger.info(response)

        response.raise_for_status()

        token_data = response.json()
        logger.info("succesfuly geerated new refresh token")
        logging.info("Successfully refreshed access token")
        
        return token_data
        
    except requests.RequestException as e:
        logging.error(f"Token refresh failed: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during token refresh: {e}")
        return None
# 