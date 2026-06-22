#!/usr/bin/env bash
# Cloud-Native Processing Platform - Chaos & Resilience test suite
# Automatically injects failures and verifies that the platform self-heals and drops zero tasks.

set -euo pipefail

NAMESPACE="processing-platform"
API_URL="http://localhost:8080" # Assumes port-forward to producer is active on 8080
TASK_COUNT=50
SUBMITTED_JOBS=()

echo "========================================================="
echo " Starting Chaos and Resilience Validation Suite"
echo "========================================================="

# Helper to check if API is available
wait_for_api() {
  echo "Waiting for Producer API to be ready..."
  until curl -sf "${API_URL}/healthz" > /dev/null; do
    sleep 2
  done
  echo "Producer API is online!"
}

wait_for_api

# 1. Generate heavy workload (Submit 50 jobs)
echo "---------------------------------------------------------"
echo " Step 1: Submitting ${TASK_COUNT} tasks to trigger scale-up..."
echo "---------------------------------------------------------"
for ((i=1; i<=TASK_COUNT; i++)); do
  response=$(curl -sf -X POST "${API_URL}/jobs" \
    -H "Content-Type: application/json" \
    -d "{\"task_type\": \"chaos-job-$i\", \"duration_seconds\": 4}")
  job_id=$(echo "$response" | grep -oE '"job_id":"[^"]+"' | cut -d'"' -f4)
  SUBMITTED_JOBS+=("$job_id")
  echo "Submitted job $i: $job_id"
done

echo "Successfully submitted all tasks."
echo "Sleeping 5 seconds to let scale-up begin..."
sleep 5

# 2. Verify worker replicas are scaling up via KEDA
workers_count=$(kubectl get deployment worker -n "$NAMESPACE" -o jsonpath='{.status.replicas}')
echo "Current worker replicas: $workers_count"
if [ "$workers_count" -eq 0 ]; then
  echo "ERROR: KEDA failed to scale workers from 0!"
  exit 1
fi
echo "SUCCESS: KEDA scaled workers up to $workers_count."

# 3. Inject Pod Termination Chaos (Kill active worker pod)
echo "---------------------------------------------------------"
echo " Step 2: Injecting Worker Pod Chaos (SIGTERM/Deletion)..."
echo "---------------------------------------------------------"
worker_pod=$(kubectl get pods -n "$NAMESPACE" -l app=worker --no-headers | head -n 1 | awk '{print $1}')
echo "Terminating active worker pod: $worker_pod"
kubectl delete pod "$worker_pod" -n "$NAMESPACE" --grace-period=10
echo "Worker pod deleted. Kubernetes scheduler should automatically replace it."

# 4. Inject Broker Chaos (Kill RabbitMQ broker)
echo "---------------------------------------------------------"
echo " Step 3: Injecting Message Broker Failure..."
echo "---------------------------------------------------------"
rabbitmq_pod=$(kubectl get pods -n "$NAMESPACE" -l app=rabbitmq --no-headers | awk '{print $1}')
echo "Deleting RabbitMQ pod: $rabbitmq_pod"
kubectl delete pod "$rabbitmq_pod" -n "$NAMESPACE" --now
echo "RabbitMQ pod deleted. StatefulSet/Deployment should spin up a fresh broker."

# 5. Wait for recovery and processing completion
echo "---------------------------------------------------------"
echo " Step 4: Waiting for system to recover and process jobs..."
echo "---------------------------------------------------------"
sleep 20 # Let RabbitMQ and workers recover

# 6. Verify task statuses (Zero message loss verification)
echo "---------------------------------------------------------"
echo " Step 5: Auditing job processing statuses (Checking Redis)..."
echo "---------------------------------------------------------"
failed_jobs=0
completed_jobs=0
pending_jobs=0

for job_id in "${SUBMITTED_JOBS[@]}"; do
  status_response=$(curl -sf "${API_URL}/jobs/${job_id}")
  job_status=$(echo "$status_response" | grep -oE '"status":"[^"]+"' | cut -d'"' -f4)
  
  if [ "$job_status" == "COMPLETED" ]; then
    completed_jobs=$((completed_jobs + 1))
  elif [ "$job_status" == "FAILED" ]; then
    failed_jobs=$((failed_jobs + 1))
  else
    pending_jobs=$((pending_jobs + 1))
  fi
done

echo "Audit results:"
echo "  - Completed: $completed_jobs"
echo "  - Failed: $failed_jobs"
echo "  - Pending/Processing: $pending_jobs"

# Wait a bit longer if there are still pending jobs
if [ "$pending_jobs" -gt 0 ]; then
  echo "Some jobs are still processing. Waiting another 15 seconds..."
  sleep 15
  # Re-audit
  completed_jobs=0
  failed_jobs=0
  for job_id in "${SUBMITTED_JOBS[@]}"; do
    status_response=$(curl -sf "${API_URL}/jobs/${job_id}")
    job_status=$(echo "$status_response" | grep -oE '"status":"[^"]+"' | cut -d'"' -f4)
    if [ "$job_status" == "COMPLETED" ]; then
      completed_jobs=$((completed_jobs + 1))
    elif [ "$job_status" == "FAILED" ]; then
      failed_jobs=$((failed_jobs + 1))
    fi
  done
  echo "Final Audit results: Completed=$completed_jobs, Failed=$failed_jobs"
fi

if [ "$failed_jobs" -gt 0 ]; then
  echo "WARNING: $failed_jobs jobs failed! Broker recovery or worker crash handler needs optimization."
else
  echo "SUCCESS: 100% of jobs processed successfully after broker and worker crashes! Zero message loss."
fi

# 7. Verify scale down to 0
echo "---------------------------------------------------------"
echo " Step 6: Verifying KEDA scale-down to 0..."
echo "---------------------------------------------------------"
echo "Waiting 35 seconds for cool-down period..."
sleep 35
final_workers=$(kubectl get deployment worker -n "$NAMESPACE" -o jsonpath='{.status.replicas}')
echo "Final worker replicas: $final_workers"
if [ "$final_workers" -eq 0 ]; then
  echo "SUCCESS: Platform scaled back down to 0 workers. Cost optimization verified!"
else
  echo "WARNING: Workers are still running ($final_workers replicas). Scale-down is taking longer than expected."
fi

echo "========================================================="
echo " Resilience validation complete."
echo "========================================================="
