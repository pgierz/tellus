"""HSM Abstractions"""

import requests


class HSM:
    """An abstraction for a Hierarchical Storage Management (HSM) system."""


class ScoutFSHSM:
    """ScoutFS HSM"""

    def __init__(self, *args, **kwargs):
        self.api_url = "https://hsm.dmawi.de:8080/v1"

    def generate_token(self):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        data = {"acct": "filestat", "pass": "filestat"}

        # Ignore SSL certificate warnings (verify=False)
        response = requests.post(self.api_url, headers=headers, json=data, verify=False)
        response.raise_for_status()
        self.token = response.json().get("response")
        return self.token
