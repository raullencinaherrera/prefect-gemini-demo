CMDB update process

When a device field must be updated in CMDB, do not write directly to CMDB.

Use the Boomi integration endpoint.

Boomi endpoint:
POST https://boomi.example.internal/api/cmdb/update

Headers:
- Content-Type: application/json
- X-API-Key: use environment variable BOOMI_API_KEY

Expected payload:
{
  "device_id": "<device_id>",
  "field_name": "<field_name>",
  "field_value": "<field_value>",
  "source": "prefect_ai_remediation"
}

Rules:
- Always log the outgoing payload excluding secrets
- Retry up to 3 times on timeout
- Raise an exception if response code is not 200
- Parse JSON response and log remediation result
