# QueueCTL - Job Queue Management CLI

# 🧭 QueueCTL — Lightweight Job Queue & Worker CLI

**QueueCTL** is a lightweight, Python-based command-line tool that simulates a **task queue system** with job processing, configuration management, and a dead letter queue (DLQ).

It’s built using only the Python standard library (plus `click` for the CLI) and a simple SQLite database for storage.

---

## ✨ Features Implemented

### 🖥️ Core Functionality (Web/CLI Equivalent)
- Enqueue and manage jobs using simple JSON inputs.
- Start one or multiple worker threads to process jobs concurrently.
- Persistent job tracking using **SQLite database**.
- Support for **Dead Letter Queue (DLQ)** for failed jobs.
- View job statistics and overall queue status.
- Configurable system parameters through the CLI.

### ⚙️ Internal Architecture
- Modular codebase divided into:
  - `queuectl.py` → CLI entry point (built with `click`)
  - `database.py` → Handles SQLite database interactions
  - `job_manager.py` → Core logic for job enqueue, dequeue, retry, DLQ
  - `worker.py` → Worker threads that process queued jobs
  - `config_manager.py` → Configuration persistence and access

---

## 🧩 Project Structure

queuectl/
│
├── queuectl.py # CLI entry point
├── database.py # SQLite database management
├── job_manager.py # Job handling and DLQ logic
├── worker.py # Worker simulation for job execution
├── config_manager.py # Configuration management
├── queue.db # Auto-created SQLite DB
├── Screenshots/ # Screenshots for documentation
└── README.md # Project documentation

---

## 🛠️ Setup Instructions

### 1️⃣ Create and activate a virtual environment
```bash
python -m venv venv
venv\Scripts\activate      # On Windows

install dependencies
pip install click

initialize the database
The database initializes automatically when the CLI is first run.

##⚡ Usage Guide

View available commands
python queuectl.py --help

Enqueue a new job
python queuectl.py job enqueue '{""command"": ""echo Hello World""}'

List all jobs
python queuectl.py job list

Check job status summary
python queuectl.py job status

Start workers
python queuectl.py worker start --count 1

##📸 Screenshots
<img width="948" height="740" alt="Demo_Screenshot" src="https://github.com/user-attachments/assets/b584c84e-a488-43d4-b7ab-31b5b88f88f3" />

##🧰 Tech Stack

Language: Python
Framework: Click (for CLI interface)
Database: SQLite
Architecture: Modular (job manager, worker, config, and DB)

