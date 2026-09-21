"""Emulate wind turbines sending telemetry to Azure Event Hubs.

The connection string is read from EH_CONNECTION_STR (environment variable or .env file).
EH_NAME is only needed when the connection string has no EntityPath.
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv

from common.logging_utils import get_logger

log = get_logger("producer")


def make_event(device_count: int) -> dict:
    return {
        "device_id": f"WT-{random.randint(1, device_count):04d}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "wind_speed_mps": round(random.uniform(0, 25), 2),
        "rotor_rpm": round(random.uniform(0, 20), 2),
        "power_output_kw": round(random.uniform(0, 3000), 1),
        "nacelle_temp_c": round(random.uniform(-10, 60), 1),
        "status": random.choice(["OK", "OK", "OK", "WARN", "FAULT"]),
    }


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--count", type=int, default=300,
                        help="number of events to send, 0 runs until stopped")
    parser.add_argument("--devices", type=int, default=50, help="number of simulated turbines")
    args = parser.parse_args()

    connection_str = os.environ.get("EH_CONNECTION_STR")
    if not connection_str:
        raise SystemExit("Set EH_CONNECTION_STR in .env or the environment")

    client = EventHubProducerClient.from_connection_string(
        connection_str, eventhub_name=os.environ.get("EH_NAME")
    )

    sent = 0
    log.info("Sending %s events at %.1f events/s", args.count or "unlimited", args.rate)
    try:
        with client:
            while args.count == 0 or sent < args.count:
                batch = client.create_batch()
                batch.add(EventData(json.dumps(make_event(args.devices))))
                client.send_batch(batch)
                sent += 1
                if sent % 50 == 0:
                    log.info("Sent %d events", sent)
                time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        log.info("Stopped by user")
    log.info("Done, total events sent: %d", sent)


if __name__ == "__main__":
    main()
