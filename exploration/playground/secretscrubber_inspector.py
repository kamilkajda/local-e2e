import os
import re
import textwrap


# Kopiujemy bezpośrednio Twoją klasę/metodę do testu (lub importujemy, jeśli masz to w pakiecie)
def scrub_secrets_test(text):
    patterns = [
        r'(?i)(api[_-]?key|password|secret|token|account[_-]?key)\s*[:=]\s*["\'][^"\']+["\']',
        r'(?i)(["\']connection[_-]?string["\'])\s*[:=]\s*["\'][^"\']+["\']',
    ]
    scrubbed = text
    for pattern in patterns:
        scrubbed = re.sub(pattern, r'\1: "**** REDACTED_FOR_SECURITY ****"', scrubbed)
    return scrubbed


# --- TEST CASE ---
test_content = """
{
    "api_key": "12345-ABCDE-SECRET-KEY",
    "db_password": "super_secure_password_123",
    "connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=very_long_key_here;EndpointSuffix=core.windows.net",
    "normal_field": "public_value",
    "ACCOUNT-KEY": 'hidden_key_99'
}
"""

print("=== ORIGINAL CONTEXT ===")
print(test_content)

print("\n=== SCRUBBED CONTEXT (What the Agent Sees) ===")
print(scrub_secrets_test(test_content))
