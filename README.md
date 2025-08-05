## Salesforce OAuth Integration App

This application integrates with Salesforce using OAuth 2.0 and offers both **data fetching** and **real-time updates via webhooks**. It authenticates users via Salesforce, retrieves and stores their data (including custom fields and metadata), and listens to webhook notifications from Salesforce to keep the database in sync.

---

##  Features

-  OAuth 2.0 Authentication with Salesforce
-  Secure access token and instance URL storage
-  SOQL-based data extraction (including standard & custom fields)
-  Metadata extraction for custom fields
-  Webhook listener for real-time updates from Salesforce
-  Automatic parsing of webhook XML → JSON
-  Intelligent upsert logic to update or insert records in DB
-  All retrieved or updated data stored in local database

---

##  Workflow Overview

### 1. **OAuth Authentication**

- `GET /connect`  
  Redirects the user to Salesforce login.
  
- Salesforce returns an authorization code.
- Code is exchanged for:
  - `access_token`
  - `instance_url`
- Tokens are securely stored in the database.

### 2. **Data Fetching from Salesforce**

- Using the `access_token` and `instance_url`, SOQL queries are made.
- Both standard and **custom fields** are fetched.
- Metadata is extracted to understand custom field structure.
- Data is stored in the database.

### 3. **Webhook Integration**

- Salesforce sends webhook (Outbound Message) in **XML format**.
- App listens on a `/webhook` endpoint.
- XML is parsed into JSON.
- Data is **upserted** into the database (insert or update if exists).
- Keeps local DB in sync with Salesforce in near real-time.

---

## 🔌 API Endpoints

| Method | Endpoint     | Description                                      |
|--------|--------------|--------------------------------------------------|
| GET    | `/connect`   | Initiates Salesforce OAuth login                 |
| GET    | `/callback`  | Handles OAuth redirect and fetches tokens        |
| POST   | `/salesforce-sync`   | Handles data extraction via soql queries |
| POST   | `/webhook`   | Receives XML payloads from Salesforce Webhooks   |

---


<!-- ## 📁 Project Structure (Example) -->

