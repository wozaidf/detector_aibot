import httpx
from config import get_settings

settings = get_settings()

url = "https://api.typesafe.ai/v1/systemone"
api_key = settings.JEV_API_KEY
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

def handle_exception_decision(jev):
    payload = {
        "state": jev.get("state"),
        "model": settings.JEV_NAME,
        "questions": jev.get("questions")
    }
    try:
        response = httpx.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as exc:
        print(f"An error occurred while requesting {exc.request.url!r}.")
        print(f"Details: {exc}")
    except httpx.HTTPStatusError as exc:
        print(f"Error response {exc.response.status_code} while requesting {exc.request.url!r}.")
        print(f"Response content: {exc.response.text}")
