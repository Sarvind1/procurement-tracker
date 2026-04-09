import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def send_simple_slack_message(meesage_text):
    client = WebClient(token=os.environ.get('SLACK_BOT_TOKEN'))
    channel_id = "C09DBM7L7ND"

    try:
        client.chat_postMessage(
            channel=channel_id,
            text=meesage_text
        )
        print("Slack message sent!")
    except SlackApiError as e:
        print(f"Slack Error: {e.response['error']}")