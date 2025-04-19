import json
import re


def make_json_pretty(data):
    json_str = json.dumps(data, indent=2)
    print(json_str)
    return json_str


def extract_id_from_url(url):
    match = re.search(r"/([^/]+)$", url)
    if match:
        return match.group(1)
    return None
