import httpx
from config.settings import GAMMA_API_URL

async def fetch_active_events(tag_id):
    url = f"{GAMMA_API_URL}/events?active=true&closed=false&tag_id={tag_id}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()