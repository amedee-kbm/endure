#!/usr/bin/env bash
# E4 orchestration: cold-start the stack at each worker count, run the sweep,
# collect CSVs. "Cold start" means docker compose down -v (volumes included)
# so source_files does not accumulate across scales.
#
# Usage (from the repo root):
#   bash src/evaluate/run_e4_sweep.sh
#
# Output: loadtest-results/e4/w{N}_{timestamp}.csv for each worker count.

set -euo pipefail

COMPOSE_FILES="-f docker-compose.yml -f docker-compose.evaluate.yml"
WORKER_COUNTS=(1 2 4)

mkdir -p loadtest-results/e4

for W in "${WORKER_COUNTS[@]}"; do
  echo ""
  echo "========================================================"
  echo " E4: cold-starting stack with --scale worker=${W}"
  echo "========================================================"

  # Full teardown including volumes (wipes DB so source_files starts empty)
  docker compose ${COMPOSE_FILES} down --volumes --remove-orphans 2>/dev/null || true

  # Fresh stack at the desired scale
  docker compose ${COMPOSE_FILES} up -d \
    --scale worker="${W}" \
    --wait

  echo "[E4] stack ready (workers=${W}); running measurement..."

  docker compose ${COMPOSE_FILES} run --rm \
    -e ENDURE_E4_WORKERS="${W}" \
    runner \
    pytest src/evaluate/test_e4_worker_sweep.py -v --tb=short

  echo "[E4] w=${W} done"
done

# Final teardown
docker compose ${COMPOSE_FILES} down --volumes --remove-orphans 2>/dev/null || true

echo ""
echo "All E4 configurations complete. Results in loadtest-results/e4/"
ls -lh loadtest-results/e4/
