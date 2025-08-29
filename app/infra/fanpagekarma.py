import requests
import os

BASE_URL = "http://app.fanpagekarma.com/api/v1"
TOKEN = os.getenv("FANPAGEKARMA_TOKEN")   # ⬅️ agora vem do ENV

def fetch_posts(network, handle, start_date, end_date):
    params = {"token": TOKEN, "period": f"{start_date}_{end_date}"}
    resp = requests.get(f"{BASE_URL}/{network}/{handle}/posts", params=params)
    resp.raise_for_status()
    return resp.json()
