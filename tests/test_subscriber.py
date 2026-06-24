# tests/test_subscriber.py - Unit tests for subscriber.py

import re
import sys
import signal as signal_module
import threading
from datetime import datetime, time
from unittest.mock import MagicMock, patch, call

import pytest
import redis
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis import given, settings
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_subscriber_state():
    """Reset the module-level shutdown_event in subscriber before each test.

    The global shutdown_event in subscriber.py persists across tests within
    the same process. Tests that set it (e.g. shutdown tests) would cause
    subsequent tests to exit immediately without this reset.
    """
    import subscriber
    subscriber.shutdown_event.clear()
    yield
    subscriber.shutdown_event.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_redis_mock(ping_side_effect=None, subscribe_side_effect=None):
    """Return (client_mock, pubsub_mock) with optional error injection."""
    client_mock = MagicMock()
    pubsub_mock = MagicMock()

    if ping_side_effect is not None:
        client_mock.ping.side_effect = ping_side_effect
    if subscribe_side_effect is not None:
        pubsub_mock.subscribe.side_effect = subscribe_side_effect

    client_mock.pubsub.return_value = pubsub_mock
    return client_mock, pubsub_mock


# ---------------------------------------------------------------------------
# connect_and_subscribe tests
# ---------------------------------------------------------------------------


class TestConnectAndSubscribe:
    """Tests for connect_and_subscribe()."""

    def test_ping_connection_error_exits_1(self):
        """ConnectionError from ping() should call sys.exit(1)."""
        client_mock, _ = _make_redis_mock(
            ping_side_effect=redis.exceptions.ConnectionError("refused")
        )

        with patch("redis.Redis", return_value=client_mock):
            from subscriber import connect_and_subscribe

            with pytest.raises(SystemExit) as exc_info:
                connect_and_subscribe()

        assert exc_info.value.code == 1

    def test_subscribe_connection_error_exits_1(self):
        """ConnectionError from subscribe() should call sys.exit(1)."""
        client_mock, _ = _make_redis_mock(
            subscribe_side_effect=redis.exceptions.ConnectionError("refused")
        )

        with patch("redis.Redis", return_value=client_mock):
            from subscriber import connect_and_subscribe

            with pytest.raises(SystemExit) as exc_info:
                connect_and_subscribe()

        assert exc_info.value.code == 1

    def test_ping_connection_error_prints_to_stderr(self, capsys):
        """ConnectionError from ping() should print an error to stderr."""
        client_mock, _ = _make_redis_mock(
            ping_side_effect=redis.exceptions.ConnectionError("connection refused")
        )

        with patch("redis.Redis", return_value=client_mock):
            from subscriber import connect_and_subscribe

            with pytest.raises(SystemExit):
                connect_and_subscribe()

        captured = capsys.readouterr()
        assert "Error" in captured.err

    def test_successful_connection_returns_pubsub(self):
        """Successful connect and subscribe returns the pubsub object."""
        client_mock, pubsub_mock = _make_redis_mock()

        with patch("redis.Redis", return_value=client_mock):
            from subscriber import connect_and_subscribe
            result = connect_and_subscribe()

        assert result is pubsub_mock
        client_mock.ping.assert_called_once()
        pubsub_mock.subscribe.assert_called_once_with("demo-channel")


# ---------------------------------------------------------------------------
# listen_loop tests
# ---------------------------------------------------------------------------


class TestListenLoop:
    """Tests for listen_loop()."""

    def test_connection_error_during_get_message_exits_1(self):
        """ConnectionError from get_message() should exit with code 1."""
        pubsub_mock = MagicMock()
        pubsub_mock.get_message.side_effect = redis.exceptions.ConnectionError("lost")

        shutdown_event = threading.Event()

        from subscriber import listen_loop
        with pytest.raises(SystemExit) as exc_info:
            listen_loop(pubsub_mock, shutdown_event)

        assert exc_info.value.code == 1

    def test_connection_error_prints_to_stderr(self, capsys):
        """ConnectionError from get_message() prints an error to stderr."""
        pubsub_mock = MagicMock()
        pubsub_mock.get_message.side_effect = redis.exceptions.ConnectionError("lost connection")

        shutdown_event = threading.Event()

        from subscriber import listen_loop
        with pytest.raises(SystemExit):
            listen_loop(pubsub_mock, shutdown_event)

        captured = capsys.readouterr()
        assert "Error" in captured.err

    def test_bytes_payload_formatted_and_printed(self, capsys):
        """A message dict with bytes data should be formatted and printed to stdout."""
        pubsub_mock = MagicMock()
        shutdown_event = threading.Event()

        message = {
            "type": "message",
            "channel": b"demo-channel",
            "data": b"Message #1 at 14:32:01",
        }

        call_count = [0]

        def get_message_side_effect(timeout=0.1):
            call_count[0] += 1
            if call_count[0] == 1:
                return message
            # Set shutdown so the loop exits cleanly on the next check
            shutdown_event.set()
            return None

        pubsub_mock.get_message.side_effect = get_message_side_effect

        from subscriber import listen_loop
        with pytest.raises(SystemExit) as exc_info:
            listen_loop(pubsub_mock, shutdown_event)

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "[RECEIVED]" in captured.out
        assert "Message #1 at 14:32:01" in captured.out

    def test_shutdown_event_exits_0(self):
        """When shutdown_event is set before entering loop, exits with code 0."""
        pubsub_mock = MagicMock()
        pubsub_mock.get_message.return_value = None  # No messages

        shutdown_event = threading.Event()
        shutdown_event.set()  # Set before entering loop

        from subscriber import listen_loop
        with pytest.raises(SystemExit) as exc_info:
            listen_loop(pubsub_mock, shutdown_event)

        assert exc_info.value.code == 0

    def test_non_message_type_is_ignored(self, capsys):
        """subscribe-type messages (confirmation) should not be printed to stdout."""
        pubsub_mock = MagicMock()
        shutdown_event = threading.Event()

        call_count = [0]

        def get_message_side_effect(timeout=0.1):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"type": "subscribe", "channel": b"demo-channel", "data": 1}
            shutdown_event.set()
            return None

        pubsub_mock.get_message.side_effect = get_message_side_effect

        from subscriber import listen_loop
        with pytest.raises(SystemExit):
            listen_loop(pubsub_mock, shutdown_event)

        captured = capsys.readouterr()
        assert "[RECEIVED]" not in captured.out


# ---------------------------------------------------------------------------
# main() tests
# ---------------------------------------------------------------------------


class TestMain:
    """Tests for main()."""

    def test_keyboard_interrupt_exits_0(self):
        """KeyboardInterrupt raised inside main() results in sys.exit(0)."""
        client_mock, pubsub_mock = _make_redis_mock()

        # Raise KeyboardInterrupt when listen_loop is called
        def fake_listen_loop(pubsub, ev):
            raise KeyboardInterrupt()

        with patch("redis.Redis", return_value=client_mock), \
             patch("signal.signal"), \
             patch("subscriber.listen_loop", side_effect=fake_listen_loop):

            from subscriber import main
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 0

    def test_sigterm_and_sigint_registered(self):
        """main() registers _shutdown for both SIGTERM and SIGINT."""
        client_mock, pubsub_mock = _make_redis_mock()

        def fake_listen_loop(pubsub, ev):
            raise SystemExit(0)

        with patch("redis.Redis", return_value=client_mock), \
             patch("signal.signal") as mock_signal, \
             patch("subscriber.listen_loop", side_effect=fake_listen_loop):

            from subscriber import main
            with pytest.raises(SystemExit):
                main()

        registered_signals = {c[0][0] for c in mock_signal.call_args_list}
        assert signal_module.SIGTERM in registered_signals
        assert signal_module.SIGINT in registered_signals

    def test_listening_confirmation_printed(self, capsys):
        """main() prints a confirmation mentioning demo-channel after connecting."""
        client_mock, pubsub_mock = _make_redis_mock()

        def fake_listen_loop(pubsub, ev):
            raise SystemExit(0)

        with patch("redis.Redis", return_value=client_mock), \
             patch("signal.signal"), \
             patch("subscriber.listen_loop", side_effect=fake_listen_loop):

            from subscriber import main
            with pytest.raises(SystemExit):
                main()

        captured = capsys.readouterr()
        assert "demo-channel" in captured.out


# ---------------------------------------------------------------------------
# Property-based tests for format_received
# ---------------------------------------------------------------------------

# Feature: redis-pubsub-demo, Property 4: Subscriber handles arbitrary payloads without raising
class TestFormatReceivedProperty4:
    """Property 4: format_received() never raises and always returns str."""

    @settings(max_examples=100)
    @given(
        payload=st.one_of(
            st.text(),
            st.binary(),
            st.none(),
            st.integers(),
            st.floats(allow_nan=True),
        )
    )
    def test_format_received_arbitrary_payload_returns_str_no_exception(self, payload):
        """**Validates: Requirements 5.5**

        For any value passed as payload — including empty strings, non-string
        types, bytes, integers, floats, or None — format_received() SHALL
        return a str and SHALL NOT raise an exception.
        """
        from subscriber import format_received

        result = format_received(payload)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Property-based tests for format_received
# ---------------------------------------------------------------------------

# Feature: redis-pubsub-demo, Property 3: Subscriber log format correctness

class TestFormatReceivedProperty3:
    """Property 3: Subscriber log format correctness.

    Validates: Requirements 5.1
    """

    @given(
        payload=st.text(min_size=1),
        fake_time=st.times(),
    )
    @settings(max_examples=100)
    def test_format_received_log_format_correctness(self, payload, fake_time):
        """Property 3: For any non-empty string payload and any valid local time,
        format_received(payload) SHALL produce a string starting with '[RECEIVED] ',
        containing a valid HH:MM:SS timestamp, the separator ' — ', and ending
        with the exact payload.

        Validates: Requirements 5.1
        """
        # Build a datetime whose time component matches the Hypothesis-generated time
        fake_dt = datetime(2024, 1, 1,
                           fake_time.hour,
                           fake_time.minute,
                           fake_time.second)

        with patch("subscriber.datetime") as mock_dt:
            mock_dt.now.return_value = fake_dt

            from subscriber import format_received
            result = format_received(payload)

        # Must match the overall format pattern
        pattern = r"^\[RECEIVED\] \d{2}:\d{2}:\d{2} \u2014 .+$"
        assert re.match(pattern, result, re.DOTALL), (
            f"Result {result!r} does not match expected pattern"
        )

        # Must end with the exact payload
        assert result.endswith(payload), (
            f"Result {result!r} does not end with payload {payload!r}"
        )
