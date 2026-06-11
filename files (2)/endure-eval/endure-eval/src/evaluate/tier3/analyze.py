"""
Post-processing for the throughput sweeps: speedup and efficiency tables.

  python src/evaluate/tier3/analyze.py loadtest-results/e4a   # sleep sweep
  python src/evaluate/tier3/analyze.py loadtest-results/e4    # real-job sweep

Reads every CSV with `worker_count` and `makespan_s` columns; the baseline is
the mean makespan at the lowest worker count. Prints a table and writes
summary.csv next to the inputs. S(N) = makespan(base)/makespan(N);
E(N) = S(N) * base / N.
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean


def main(directory: str) -> None:
    root = Path(directory)
    makespans: dict[int, list[float]] = defaultdict(list)

    for path in sorted(root.glob("*.csv")):
        if path.name in ("summary.csv",) or path.name.startswith("drain_"):
            continue
        with path.open() as fh:
            seen: set[tuple[int, float]] = set()
            for row in csv.DictReader(fh):
                if "worker_count" not in row or "makespan_s" not in row:
                    break
                key = (int(row["worker_count"]), float(row["makespan_s"]))
                if key not in seen:  # one makespan per (config, run)
                    seen.add(key)
                    makespans[key[0]].append(key[1])

    if not makespans:
        sys.exit(f"no sweep CSVs with worker_count/makespan_s under {root}")

    base_n = min(makespans)
    base = mean(makespans[base_n])

    out_rows = []
    header = f"{'N':>3} {'runs':>4} {'makespan(s)':>12} {'speedup':>8} {'efficiency':>10}"
    print(header)
    print("-" * len(header))
    for n in sorted(makespans):
        m = mean(makespans[n])
        speedup = base / m
        efficiency = speedup * base_n / n
        print(f"{n:>3} {len(makespans[n]):>4} {m:>12.1f} {speedup:>8.2f} {efficiency:>10.2f}")
        out_rows.append({
            "worker_count": n, "runs": len(makespans[n]),
            "mean_makespan_s": round(m, 2),
            "all_makespans_s": ";".join(f"{x:.1f}" for x in makespans[n]),
            "speedup": round(speedup, 3), "efficiency": round(efficiency, 3),
        })

    with (root / "summary.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(out_rows[0]))
        writer.writeheader()
        writer.writerows(out_rows)
    print(f"\nwrote {root / 'summary.csv'}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "loadtest-results/e4a")
