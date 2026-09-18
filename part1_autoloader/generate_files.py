"""Generate synthetic wind-turbine telemetry JSON files for Auto Loader.

Runs locally.
Each file holds a single record, so rows per batch equal files per batch.
"""
import argparse
import json
import logging
import os
import random
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("generator")


def make_record(add_new_column: bool, corrupt: bool) -> dict:
    rec = {
        "device_id": f"WT-{random.randint(1, 50):04d}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "wind_speed_mps": round(random.uniform(0, 25), 2),
        "rotor_rpm": round(random.uniform(0, 20), 2),
        "power_output_kw": round(random.uniform(0, 3000), 1),
        "nacelle_temp_c": round(random.uniform(-10, 60), 1),
        "status": random.choice(["OK", "OK", "OK", "WARN", "FAULT"]),
    }
    if add_new_column:
        rec["blade_pitch_deg"] = round(random.uniform(0, 90), 1)
    if corrupt:
        # Wrong type -> ends up in the _rescued_data column instead of failing the stream.
        rec["wind_speed_mps"] = "not_a_number"
    return rec


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--count", type=int, default=1000)
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--add-new-column", action="store_true")
    p.add_argument("--corrupt-index", type=int, default=-1,
                   help="file index to corrupt for the _rescued_data demo")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    for i in range(args.start, args.start + args.count):
        rec = make_record(args.add_new_column, corrupt=(i == args.corrupt_index))
        with open(os.path.join(args.out, f"turbine_{i:05d}.json"), "w") as f:
            json.dump(rec, f)

    log.info("Generated %d files in %s (new_column=%s, corrupt_index=%d)",
             args.count, args.out, args.add_new_column, args.corrupt_index)


if __name__ == "__main__":
    main()