"""Generate a SMALL SYNTHETIC test file in the same format as the UCI dataset.

IMPORTANT: this data is artificial. It exists only so the pipeline and unit tests can
run without downloading the real 127 MB dataset. Its anomalies were injected on
purpose and must never be presented as results about real households.

    python tests/generate_sample_data.py            # rewrites data/sample_electricity.csv
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

HEADER = ("Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;"
          "Sub_metering_1;Sub_metering_2;Sub_metering_3")

# (day_index, hour) -> extra kW added to every minute of that hour
INJECTED_SPIKES = {(8, 3): 2.5}          # unusual night-time surge
INJECTED_DROPS = {(9, 19): -1.2}         # evening demand much lower than usual


def _profile(hour: int) -> float:
    """Typical household shape: low at night, morning and evening peaks (kW)."""
    return (0.35 + 0.6 * np.exp(-((hour - 7.5) ** 2) / 4.0)
            + 1.1 * np.exp(-((hour - 20.0) ** 2) / 6.0))


def generate_lines(days: int = 10, seed: int = 7, start: datetime = datetime(2007, 1, 1)) -> list:
    rng = np.random.default_rng(seed)
    lines = [HEADER]
    hour_effect = rng.normal(0, 0.08, size=(days, 24))
    for d in range(days):
        for h in range(24):
            level = _profile(h) + hour_effect[d, h]
            level += INJECTED_SPIKES.get((d, h), 0.0) + INJECTED_DROPS.get((d, h), 0.0)
            minute_noise = rng.normal(0, 0.12, size=60)
            for m in range(60):
                ts = start + timedelta(days=d, hours=h, minutes=m)
                gap = max(0.08, level + minute_noise[m])
                react = abs(rng.normal(0.12, 0.02))
                volt = rng.normal(240.0, 1.5)
                inten = gap * 4.2
                row = [f"{ts.day}/{ts.month}/{ts.year}", ts.strftime("%H:%M:%S"),
                       f"{gap:.3f}", f"{react:.3f}", f"{volt:.2f}", f"{inten:.1f}", "0.000", "1.000", "17.000"]
                # Missing block (as in the real file, written as '?') on day 5, 10:00-10:19
                if d == 5 and h == 10 and m < 20:
                    row[2:] = ["?"] * 7
                # Hour with too few valid minutes on day 6 (14:00-14:44 missing)
                if d == 6 and h == 14 and m < 45:
                    row[2:] = ["?"] * 7
                lines.append(";".join(row))
    # One deliberately invalid reading (negative power, on a day after the series) and one
    # unparseable timestamp
    lines.append("11/1/2007;00:00:00;-4.000;0.100;240.00;1.0;0.000;0.000;0.000")
    lines.append("not-a-date;99:99:99;1.000;0.100;240.00;4.2;0.000;0.000;0.000")
    return lines


def main() -> int:
    out = Path(__file__).resolve().parent.parent / "data" / "sample_electricity.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(generate_lines()) + "\n", encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size / 1e6:.2f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
