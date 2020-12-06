import os
from slackeventsapi import SlackEventAdapter
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()

slack_signing_secret = os.environ["SLACK_SIGNING_SECRET"]
slack_bot_token = os.environ["SLACK_BOT_TOKEN"]

slack_events_adapter = SlackEventAdapter(slack_signing_secret, "/slack/events")


@slack_events_adapter.on("user_change")
def user_change(slack_event):
    event_type = slack_event["type"]
    user = slack_event["event"]["user"]
    profile = slack_event["event"]["user"]["profile"]

    print(user)
    print(profile["status_text"])

    if profile["status_text"] == 'In a meeting • Google Calendar':
        # end_time timedelta() datetime.now datetime.now() duration = pd.Timestamp(, unit='s', tz='US/Eastern') -
        #  pd.Timestamp(pd.Timestamp.now(), unit='s', tz='US/Eastern')
        secs = 3600  # duration.total_seconds()
        mins = int(secs / 60)
        slack_user.dnd.set_snooze(num_minutes=mins)
        print(slack_user.dnd.info().body)


# Error events
@slack_events_adapter.on("error")
def error_handler(err):
    print("ERROR: " + str(err))


if __name__ == "__main__":
    slack_events_adapter.start(port=5000)
