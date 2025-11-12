import uuid
from datetime import datetime
import database


def create_job(data):
    """
    Create a new job and insert it into the jobs table.
    """
    job_id = data.get("id") or str(uuid.uuid4())
    command = data.get("command")
    if not command:
        raise ValueError("Job must include a 'command' field")

    job = {
        "id": job_id,
        "command": command,
        "state": "pending",
        "attempts": 0,
        "max_retries": int(data.get("max_retries", database.get_config("max_retries", 3))),
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }

    database.insert_job(job)
    return job


def list_jobs(state=None):
    """
    Fetch jobs filtered by state (pending, processing, completed, failed, dead)
    """
    if state:
        return database.fetch_jobs_by_state(state)
    else:
        return (
            database.fetch_jobs_by_state("pending")
            + database.fetch_jobs_by_state("processing")
            + database.fetch_jobs_by_state("completed")
            + database.fetch_jobs_by_state("failed")
        )


def move_to_dlq(job_id, reason):
    """
    Move a job to the Dead Letter Queue.
    """
    job = database.get_job(job_id)
    if job:
        database.move_to_dlq(job["id"], job["command"], reason)
        return True
    return False


def retry_from_dlq(job_id):
    """
    Move a DLQ job back into the main queue for retry.
    """
    dlq_jobs = database.fetch_all_dlq()
    for j in dlq_jobs:
        if j["id"] == job_id:
            job = {
                "id": j["id"],
                "command": j["command"],
                "state": "pending",
                "attempts": 0,
                "max_retries": int(database.get_config("max_retries", 3)),
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            database.insert_job(job)
            return True
    return False


def list_dlq():
    """
    List all jobs in the Dead Letter Queue.
    """
    return database.fetch_all_dlq()


def get_summary():
    """
    Get a summary of all job states and counts.
    """
    summary = {}
    for state in ["pending", "processing", "completed", "failed"]:
        jobs = database.fetch_jobs_by_state(state)
        summary[state] = len(jobs)
        print(f"{state.capitalize()}: {len(jobs)} jobs")
    return summary
