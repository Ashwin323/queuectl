import subprocess
import threading
import time
from datetime import datetime
import database
import job_manager
import config_manager


_stop_event = threading.Event()


def stop_workers():
    """Signal all workers to stop after current job."""
    _stop_event.set()


def worker_thread(worker_id):
    """Single worker loop: pick pending jobs, process, retry on fail."""
    print(f"[Worker {worker_id}] started")

    while not _stop_event.is_set():
        jobs = database.fetch_jobs_by_state("pending")
        if not jobs:
            time.sleep(2)
            continue

        for job in jobs:
            if _stop_event.is_set():
                break

            job_id = job["id"]
            database.update_job_state(job_id, "processing")

            print(f"[Worker {worker_id}] Processing job {job_id} → {job['command']}")

            try:
                # Execute the command
                result = subprocess.run(
                    job["command"],
                    shell=True,
                    capture_output=True,
                    text=True
                )

                if result.returncode == 0:
                    database.update_job_state(job_id, "completed")
                    print(f"[Worker {worker_id}] ✅ Completed job {job_id}")
                else:
                    handle_job_failure(worker_id, job, result.stderr)

            except Exception as e:
                handle_job_failure(worker_id, job, str(e))

        time.sleep(1)

    print(f"[Worker {worker_id}] stopped")


def handle_job_failure(worker_id, job, error_message):
    """Handle retry, exponential backoff, and DLQ transfer."""
    job_id = job["id"]
    attempts = job["attempts"] + 1
    max_retries = job["max_retries"]
    base = int(config_manager.get_config("backoff_base") or 2)
    delay = base ** attempts

    if attempts < max_retries:
        print(f"[Worker {worker_id}] ❌ Job {job_id} failed (Attempt {attempts}/{max_retries}). Retrying in {delay}s...")
        database.update_job_state(job_id, "failed", attempts)
        time.sleep(delay)
        database.update_job_state(job_id, "pending")
    else:
        print(f"[Worker {worker_id}] 💀 Job {job_id} moved to DLQ after {max_retries} attempts.")
        job_manager.move_to_dlq(job_id, error_message)


def start_workers(count=1):
    """Start N worker threads."""
    config_manager.init_config()
    threads = []
    _stop_event.clear()

    for i in range(count):
        t = threading.Thread(target=worker_thread, args=(i + 1,), daemon=True)
        t.start()
        threads.append(t)

    print(f"🚀 {count} worker(s) running. Press Ctrl+C to stop.")

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping workers gracefully...")
        stop_workers()
        for t in threads:
            t.join()
