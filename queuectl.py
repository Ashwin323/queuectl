import json
import click
import database
import job_manager
import worker
import config_manager


@click.group()
def cli():
    """QueueCTL — A lightweight job queue and worker management CLI."""
    database.init_db()
    config_manager.init_config()


# ------------------- JOB COMMANDS -------------------

@cli.group()
def job():
    """Manage jobs."""
    pass


@job.command("enqueue")
@click.argument("job_json")
def enqueue(job_json):
    """Enqueue a new job from a JSON string."""
    try:
        job_data = json.loads(job_json)
        # Use create_job instead of enqueue_job
        job = job_manager.create_job(job_data)
        click.echo(f"✅ Job '{job['id']}' enqueued successfully.")
    except json.JSONDecodeError:
        click.echo("❌ Invalid JSON format.")
    except Exception as e:
        click.echo(f"❌ Error: {e}")


@job.command("list")
@click.option("--state", type=click.Choice(["pending", "processing", "completed", "failed"]), default=None)
def list_jobs(state):
    """List jobs filtered by state (or all)."""
    if state:
        jobs = database.fetch_jobs_by_state(state)
    else:
        jobs = (
            database.fetch_jobs_by_state("pending")
            + database.fetch_jobs_by_state("processing")
            + database.fetch_jobs_by_state("completed")
            + database.fetch_jobs_by_state("failed")
        )

    if not jobs:
        click.echo("ℹ️ No jobs found.")
        return

    for j in jobs:
        click.echo(f"[{j['id']}] {j['state']} | attempts={j['attempts']} | command={j['command']}")


@job.command("status")
def status():
    """Show overall job statistics."""
    states = ["pending", "processing", "completed", "failed"]
    click.echo("📊 Job Status Summary:")
    for state in states:
        jobs = database.fetch_jobs_by_state(state)
        click.echo(f"  {state.capitalize():<12}: {len(jobs)}")


# ------------------- WORKER COMMANDS -------------------

@cli.group(name="worker")
def worker_cmd():
    """Manage workers."""
    pass


@worker_cmd.command("start")
@click.option("--count", default=1, help="Number of worker threads to run.")
def start_workers(count):
    """Start workers to process jobs."""
    worker.start_workers(count)


# ------------------- DLQ COMMANDS -------------------

@cli.group()
def dlq():
    """Manage Dead Letter Queue."""
    pass


@dlq.command("list")
def list_dlq():
    """List all DLQ entries."""
    entries = job_manager.list_dlq()
    if not entries:
        click.echo("ℹ️ DLQ is empty.")
        return
    for e in entries:
        click.echo(f"[{e['job_id']}] | error={e['error_message']} | failed_at={e['failed_at']}")


@dlq.command("retry")
@click.argument("job_id")
def retry_dlq(job_id):
    """Retry a job from the DLQ."""
    success = job_manager.retry_from_dlq(job_id)
    if success:
        click.echo(f"🔁 Retried job '{job_id}' from DLQ.")
    else:
        click.echo(f"❌ Job '{job_id}' not found in DLQ.")


# ------------------- CONFIG COMMANDS -------------------

@cli.group()
def config():
    """Manage system configuration."""
    pass


@config.command("set")
@click.argument("key")
@click.argument("value")
def set_config(key, value):
    """Set a configuration key."""
    config_manager.set_config(key, value)
    click.echo(f"⚙️ Config '{key}' set to '{value}'.")


@config.command("show")
def show_config():
    """Show current configuration values."""
    config_values = config_manager.list_config()
    if not config_values:
        click.echo("ℹ️ No config values found.")
        return
    for key, value in config_values.items():
        click.echo(f"{key} = {value}")


# ------------------- ENTRY POINT -------------------

if __name__ == "__main__":
    cli()
