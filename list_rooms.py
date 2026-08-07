import asyncio
import logging
import yaml
from pathlib import Path
from roborock.web_api import RoborockApiClient

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def fetch_and_print_rooms():
    secrets_path = Path("secrets.yaml")
    if not secrets_path.exists():
        logger.error("secrets.yaml not found! Please create it from secrets.yaml.example.")
        return

    with open(secrets_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    email = config.get("email")
    if not email:
        logger.error("Email missing in secrets.yaml.")
        return

    logger.info("Connecting to Roborock Cloud API...")
    client = RoborockApiClient(username=email)
    
    # Login and fetch home data
    user_data = await client.login_with_password(config.get("password")) if config.get("password") else await client.request_code()
    home_data = await client.get_home_data(user_data)

    found_q10 = False
    for device in home_data.devices:
        # Match Q10 / B01 protocol models
        if hasattr(device, "b01_q10_properties"):
            found_q10 = True
            device_name = device.device_info.name
            print(f"\n==========================================")
            print(f" Device: {device_name} ({device.device_info.model})")
            print(f"==========================================")

            props = getattr(device, "b01_q10_properties", None)
            if props and hasattr(props, "map") and getattr(props.map, "rooms", None):
                print(f"{'Room ID':<10} | {'Raw Name':<20}")
                print("-" * 35)
                for room in props.map.rooms:
                    room_id = getattr(room, "id", "N/A")
                    raw_name = getattr(room, "raw_name", getattr(room, "name", "Unknown"))
                    print(f"{room_id:<10} | {raw_name:<20}")
                print("-" * 35)
            else:
                logger.warning(f"No map room data payload found for {device_name}. Ensure the device has generated a cloud map.")

    if not found_q10:
        logger.warning("No Roborock Q10 (B01 protocol) device detected in your account.")

if __name__ == "__main__":
    asyncio.run(fetch_and_print_rooms())