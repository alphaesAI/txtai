from __future__ import print_function
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# --------------------------------------------
# 1. Gmail API scopes
# --------------------------------------------
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# --------------------------------------------
# 2. Correct path to credentials.json
# --------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(BASE_DIR, "credentials.json")
token_path = os.path.join(BASE_DIR, "token.json")   # store token here also

def main():
    creds = None

    # --------------------------------------------
    # 3. If token.json exists, load it
    # --------------------------------------------
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    # --------------------------------------------
    # 4. If no valid credentials, create new login flow
    # --------------------------------------------
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                creds_path, SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save the token
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    # --------------------------------------------
    # 5. Build Gmail API service
    # --------------------------------------------
    service = build('gmail', 'v1', credentials=creds)

    # --------------------------------------------
    # 6. Test: Fetch latest 10 messages
    # --------------------------------------------
    results = service.users().messages().list(
        userId='me',
        maxResults=10
    ).execute()

    messages = results.get('messages', [])

    print("Fetched message IDs:", messages)

if __name__ == '__main__':
    main()
