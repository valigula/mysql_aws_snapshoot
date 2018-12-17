import requests


class SensuManager:
    def __init__(self):
        pass

    @staticmethod
    def silence_alerting(self, p_hostname):
        url = "https://monitor.letgo.cloud/silenced"
        headers = "Content-type: application/json"
        payloads = "{subscription: client:"+p_hostname+"}"
        r = requests.post(url, payloads, headers)

        print r

    @staticmethod
    def clear_alerting(p_hostname):
        url = "https://monitor.letgo.cloud/silenced/clear"
        headers = "Content-type: application/json"
        payloads = "{subscription: client:"+p_hostname+"}"
        r = requests.post(url, payloads, headers)

        print r
