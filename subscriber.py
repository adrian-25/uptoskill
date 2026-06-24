# subscriber.py - Redis Pub/Sub subscriber script

import sys
import signal
import threading
from datetime import datetime

import redis


# Shared shutdown event for signal handlers and main loop
shutdown_event = threading.Event()


def connect_and_subscribe():
    """Connect to Redis, create a PubSub handle, and subscribe to demo-channel.

    Returns the PubSub object on success.
    Prints an error to stderr and calls sys.exit(1) on any connection or
    subscription failure (redis.exceptions.ConnectionError or TimeoutError).
    Any exception raised by print() itself is swallowed so that the exit
    always happens cleanly.
    """
    try:
        client = redis.Redis(host='localhost', port=6379, socket_connect_timeout=5)
        client.ping()
        pubsub = client.pubsub()
        pubsub.subscribe('demo-channel')
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        try:
            print(f"Error: could not connect to Redis or subscribe to demo-channel: {exc}", file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)

    return pubsub


def format_received(payload):
    """Format a received message payload into a structured log line.

    Returns a string of the form:
        [RECEIVED] HH:MM:SS — <payload>

    - bytes values are decoded with .decode('utf-8', errors='replace')
    - all other non-str types are converted via str()
    - never raises an exception
    """
    timestamp = datetime.now().strftime('%H:%M:%S')

    try:
        if isinstance(payload, bytes):
            text = payload.decode('utf-8', errors='replace')
        elif isinstance(payload, str):
            text = payload
        else:
            text = str(payload)
    except Exception:
        text = str(payload)

    return f"[RECEIVED] {timestamp} \u2014 {text}"


def listen_loop(pubsub, shutdown_event):
    """Poll for messages and print them until shutdown is requested.

    Uses get_message(timeout=0.1) so the loop stays responsive to the
    shutdown flag (checked after every call, i.e. at most 100 ms latency).
    Exits with code 0 when shutdown_event is set.
    Prints an error to stderr and exits with code 1 on ConnectionError.
    """
    while True:
        try:
            msg = pubsub.get_message(timeout=0.1)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            try:
                print(f"Error: lost connection to Redis: {exc}", file=sys.stderr)
            except Exception:
                pass
            sys.exit(1)

        if msg is not None and msg.get('type') == 'message':
            try:
                print(format_received(msg['data']))
                sys.stdout.flush()
            except Exception:
                pass

        if shutdown_event.is_set():
            sys.exit(0)


def _shutdown(signum, frame):
    """Signal handler: sets the shutdown event to trigger a clean exit."""
    shutdown_event.set()


def main():
    """Entry point: wire up signals, connect, subscribe, then listen."""
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        pubsub = connect_and_subscribe()
        print("Listening on demo-channel…")
        sys.stdout.flush()
        listen_loop(pubsub, shutdown_event)
    except KeyboardInterrupt:
        shutdown_event.set()
        sys.exit(0)


if __name__ == '__main__':
    main()
