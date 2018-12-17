import requests
import json


class SlackManager:
    def __init__(self):
        pass

    @staticmethod
    def send_message( p_message):
        url = "https://hooks.slack.com/services/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"
        headers = 'Content-type: application/json'
        payloads = p_message
        r = requests.post(url, payloads,  headers)

        print r


"""
if __name__ == "__main__":
        message = '{ "text":"Offline Snapshot Process", "attachments": [{ "author_name": "Owner: dba@letgo.com",' \
                  '"title": "volume Id","text": "volume_id_to_be_added_as_param"},' \
                  '{"title": "Database Server","text": "server_to_be_added_as_param"},' \
                  '{"title": "Status","text": "started/completed"}' \
                  ']}'

        print message
        send_message(message)
"""