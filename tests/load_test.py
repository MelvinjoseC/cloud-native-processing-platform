#!/usr/bin/env python3
"""Async load testing tool for Cloud-Native Processing Platform.

Simulates concurrent traffic spikes and batch submissions to benchmark
producer API throughput and observe KEDA scale-out / scale-to-zero events.
"""

import argparse
import asyncio
import time
import httpx


async def send_single_job(client, url, idx):
    payload = {
        "task_type": "data_processing" if idx % 2 == 0 else "image_resize",
        "duration_seconds": 3,
        "priority": (idx % 10) + 1,
        "payload": {"item_index": idx, "test_run": True},
    }
    start = time.perf_counter()
    try:
        resp = await client.post(url, json=payload, timeout=10.0)
        latency = time.perf_counter() - start
        return resp.status_code == 202, latency, resp.status_code
    except Exception:
        latency = time.perf_counter() - start
        return False, latency, 0


async def send_batch_jobs(client, url, batch_size, batch_idx):
    jobs = [
        {
            "task_type": "report_generation",
            "duration_seconds": 2,
            "priority": 5,
            "payload": {"batch_id": batch_idx, "job_seq": j},
        }
        for j in range(batch_size)
    ]
    start = time.perf_counter()
    try:
        resp = await client.post(url, json={"jobs": jobs}, timeout=15.0)
        latency = time.perf_counter() - start
        return resp.status_code == 202, latency, resp.status_code
    except Exception:
        latency = time.perf_counter() - start
        return False, latency, 0


async def run_load_test(base_url, total_jobs, concurrency, use_batch=False):
    target_endpoint = (
        f"{base_url}/api/v1/jobs/batch" if use_batch else f"{base_url}/api/v1/jobs"
    )
    print("\n--- Starting Load Test ---")
    print(f"Target:      {target_endpoint}")
    print(f"Total Jobs:  {total_jobs}")
    print(f"Concurrency: {concurrency}")
    print(f"Batch Mode:  {use_batch}\n")

    limits = httpx.Limits(
        max_connections=concurrency, max_keepalive_connections=concurrency
    )
    async with httpx.AsyncClient(limits=limits) as client:
        start_overall = time.perf_counter()

        if use_batch:
            batch_size = 10
            total_batches = max(1, total_jobs // batch_size)
            tasks = [
                send_batch_jobs(client, target_endpoint, batch_size, i)
                for i in range(total_batches)
            ]
        else:
            tasks = [
                send_single_job(client, target_endpoint, i) for i in range(total_jobs)
            ]

        results = await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_overall

    successes = sum(1 for s, _, _ in results if s)
    failures = len(results) - successes
    latencies = [lat for _, lat, _ in results]

    avg_lat = sum(latencies) / len(latencies) if latencies else 0
    p95_lat = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
    throughput = (
        total_jobs if not use_batch else total_batches * batch_size
    ) / total_time

    print("\n================ Results Summary ================")
    print(f"Total Requests:     {len(results)}")
    print(f"Successful:         {successes} ({(successes/len(results))*100:.1f}%)")
    print(f"Failed:             {failures}")
    print(f"Total Elapsed Time: {total_time:.2f}s")
    print(f"Average Latency:    {avg_lat*1000:.1f}ms")
    print(f"p95 Latency:        {p95_lat*1000:.1f}ms")
    print(f"Effective Throughput:{throughput:.1f} jobs/sec")
    print("=================================================\n")


def main():
    parser = argparse.ArgumentParser(
        description="Async Job Generator & KEDA Load Tester"
    )
    parser.add_argument(
        "--url", default="http://localhost:8080", help="Base Producer API URL"
    )
    parser.add_argument(
        "--total", type=int, default=50, help="Total number of jobs to submit"
    )
    parser.add_argument(
        "--concurrency", type=int, default=10, help="Concurrent HTTP workers"
    )
    parser.add_argument(
        "--batch", action="store_true", help="Submit jobs in batch mode"
    )
    args = parser.parse_args()

    asyncio.run(
        run_load_test(args.url.rstrip("/"), args.total, args.concurrency, args.batch)
    )


if __name__ == "__main__":
    main()
