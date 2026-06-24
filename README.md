# Redis Pub/Sub Real-Time Messaging Demo

A minimal Python demo of Redis Pub/Sub messaging using the `redis-py` library. The publisher sends a timestamped, numbered message to a Redis channel every 2 seconds; the subscriber listens and prints each one as it arrives.

---

## Prerequisites

- Python 3.8+
- Docker (to run Redis locally)

---

## Installation

Install the only Python dependency:

```bash
pip install redis
```

---

## Starting Redis

**Redis must be confirmed running before starting either script.**

Use Docker to spin up a local Redis server with the correct port mapping:

```bash
docker run -d --name redis-demo -p 6379:6379 redis:latest
```

Verify it is up:

```bash
docker ps
```

You should see `redis-demo` listed with status `Up`.

---

## Running the Demo

Open **two separate terminals** from the project directory.

**Terminal 1 — Subscriber** (start this first so it is ready to receive):

```bash
python subscriber.py
```

**Terminal 2 — Publisher:**

```bash
python publisher.py
```

---

## Expected Output

### Publisher terminal

```
Published to demo-channel: Message #1 at 14:32:01
Published to demo-channel: Message #2 at 14:32:03
Published to demo-channel: Message #3 at 14:32:05
```

### Subscriber terminal

```
Listening on demo-channel…
[RECEIVED] 14:32:01 — Message #1 at 14:32:01
[RECEIVED] 14:32:03 — Message #2 at 14:32:03
[RECEIVED] 14:32:05 — Message #3 at 14:32:05
```

---

## Stopping the Scripts

Press **Ctrl+C** in either terminal to stop the script. Both scripts handle the interrupt cleanly and exit with code 0.

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| `Error: could not connect to Redis at localhost:6379` | Redis is not running | Run the `docker run` command above and confirm `docker ps` shows it up |
| No output in subscriber | Publisher not started yet, or wrong channel | Start the publisher in a second terminal |
| `docker: name already in use` | Container from a previous run still exists | `docker rm -f redis-demo`, then re-run the `docker run` command |
