import os
from dotenv import load_dotenv
load_dotenv()
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')

AUTHORIZE_URL = os.getenv('AUTHORIZE_URL')
TOKEN_URL = os.getenv('TOKEN_URL')

SECRET_KEY = os.urandom(24)


WEBHOOK_SECRET =os.getenv('WEBHOOK_SECRET')  # Optional, for signature validation
WEBHOOK_TIMEOUT = os.getenv('WEBHOOK_TIMEOUT')  # Timeout for webhook processing
ENABLE_WEBHOOK_LOGGING = True  # Enable detailed logging