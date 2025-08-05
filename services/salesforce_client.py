import requests

def execute_soql_query(access_token, instance_url, soql):
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(
        f"{instance_url}/services/data/v60.0/query",
        headers=headers,
        params={'q': soql}
    )
    response.raise_for_status()
    return response.json().get('records', [])
