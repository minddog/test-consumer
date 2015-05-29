import json
import os
import requests
import logging

PAGERDUTY_URL = 'https://events.pagerduty.com/generic/2010-04-15/create_event.json'
PAGERDUTY_KEY = os.environ.get('PAGERDUTY_KEY', '')
logger = logging.getLogger('pagerduty')

def pagerduty_event(event_type="trigger", incident_key=None, description=None, client=None, client_url=None, service_key=PAGERDUTY_KEY):
    if not service_key:
       raise Exception("Please provide a Pagerduty Service Key")

    if not description:
        description = "Tutum Stream Issue"

    data = {
        "service_key": service_key,
        "incident_key": incident_key,
        "event_type": event_type,
        "description": description,
        "client": client,
        "client_url": client_url
    }

    headers = {'Content-type': 'application/json'}
    r = requests.post(PAGERDUTY_URL,
                     headers=headers,
                     data=json.dumps(data))

    if r.status_code == 200:
        logger.debug("Posted {} to Pagerduty.".format(event_type))
    else:
        logger.warn("{} Failed to post {} to Pagerduty. {}".format(PAGERDUTY_KEY, event_type, r.json()))
        error_code = r.json().get('error').get('code')
        message = r.json().get('error').get('message')
        errors = r.json().get('error').get('errors')
        logger.warn( "{} ({}): {}".format(message, error_code, errors) )