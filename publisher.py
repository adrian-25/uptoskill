# publisher.py - Redis Pub/Sub publisher script

import signal
import sys
import threading
import time
from datetime import datetime

import redis

# Module-level shutdown event — set by signal handler or KeyboardInterrupt handler
shutdown_event = threading.Event()


def connect():
    """Create and validate a Redis client.

    Returns a connected redis.Redis client on success.
    On connection failure, prints an error to stderr (swallowing any print
    exception) and exits with code 1.
    """
    try:
        client = redis.Redis(host='localhost', port=6379, socket_connect_timeout=5)
        client.ping()
        return client
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        try:
            print(
                f"Error: could not connect to Redis at localhost:6379 — {exc}",
                file=sys.stderr,
            )
        except Exception:
            pass
        sys.exit(1)


def build_message(counter):
    """Return a formatted message string for the given counter value.

    Format: 'Message #<N> at <HH:MM:SS>'

    Pure function — no side effects.
    """
    timestamp = datetime.now().strftime('%H:%M:%S')
    return f"Message #{counter} at {timestamp}"


def publish_loop(client, shutdown_event):
    """Continuously publish messages to demo-channel until shutdown_event is set.

    - Checks shutdown_event before each iteration; exits 0 immediately if set.
    - Acquires a threading.Lock around client.publish() to allow in-progress
      operations to complete before responding to shutdown.
    - Increments counter only after a successful publish() return.
    - Prints each message to stdout within 1 second of publish returning.
    - On redis.exceptions.ConnectionError: if shutdown was already initiated,
      exits 0; otherwise prints error to stderr (swallowing print exceptions)
      and exits 1.
    - Sleeps 2 seconds between iterations (after releasing the lock).
    """
    publish_lock = threading.Lock()
    counter = 1

    while True:
        # Check shutdown before starting a new iteration; exit immediately if set
        if shutdown_event.is_set():
            sys.exit(0)

        message = build_message(counter)

        try:
            with publish_lock:
                client.publish('demo-channel', message)
        except redis.exceptions.ConnectionError:
            # If shutdown was already initiated, prefer clean exit
            if shutdown_event.is_set():
                sys.exit(0)
            try:
                print(
                    "Error: lost connection to Redis — stopping publisher.",
                    file=sys.stderr,
                )
            except Exception:
                pass
            sys.exit(1)

        # Increment only after successful publish
        counter += 1

        # Print within 1 second of publish returning
        print(f"Published to demo-channel: {message}")

        # Sleep 2 seconds between iterations (lock already released by context manager)
        time.sleep(2)


def _shutdown(signum, frame):
    """Signal handler — sets the module-level shutdown_event."""
    shutdown_event.set()


def main():
    """Entry point: wire up signals, connect, and run the publish loop."""
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    client = connect()
    publish_loop(client, shutdown_event)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        shutdown_event.set()
        sys.exit(0)
