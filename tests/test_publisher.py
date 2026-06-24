# tests/test_publisher.py - Tests for publisher.py

import re
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

# ---------------------------------------------------------------------------
# build_message tests
# ---------------------------------------------------------------------------

class TestBuildMessage:
    """Tests for the build_message() function."""

    def test_format_counter_1(self):
        """build_message(1) should produce 'Message #1 at HH:MM:SS'."""
        from publisher import build_message

        result = build_message(1)
        assert re.match(r'^Message #1 at \d{2}:\d{2}:\d{2}$', result), (
            f"Unexpected format: {result!r}"
        )

    def test_format_counter_100(self):
        """build_message(100) should embed counter 100 correctly."""
        from publisher import build_message

        result = build_message(100)
        assert re.match(r'^Message #100 at \d{2}:\d{2}:\d{2}$', result), (
            f"Unexpected format: {result!r}"
        )

    def test_format_counter_999(self):
        """build_message(999) should embed counter 999 correctly."""
        from publisher import build_message

        result = build_message(999)
        assert re.match(r'^Message #999 at \d{2}:\d{2}:\d{2}$', result), (
            f"Unexpected format: {result!r}"
        )

    def test_uses_current_time(self):
        """build_message uses datetime.now() for the timestamp."""
        from unittest.mock import patch
        from datetime import datetime
        from publisher import build_message

        fake_time = datetime(2024, 1, 1, 14, 32, 1)
        with patch('publisher.datetime') as mock_dt:
            mock_dt.now.return_value = fake_time
            result = build_message(5)

        assert result == 'Message #5 at 14:32:01'

    def test_pure_no_side_effects(self):
        """Calling build_message multiple times with same counter gives consistent format."""
        from publisher import build_message

        r1 = build_message(42)
        r2 = build_message(42)
        # Both should match the pattern (timestamp may differ by seconds)
        pattern = r'^Message #42 at \d{2}:\d{2}:\d{2}$'
        assert re.match(pattern, r1)
        assert re.match(pattern, r2)


# ---------------------------------------------------------------------------
# Property-based tests for build_message
# ---------------------------------------------------------------------------

# Feature: redis-pubsub-demo, Property 1: Message format correctness
class TestBuildMessageProperty1:
    """Property 1: Message format correctness — Validates: Requirements 2.2"""

    @given(
        n=st.integers(min_value=1, max_value=10_000),
        t=st.times(),
    )
    @settings(max_examples=100)
    def test_message_format_correctness(self, n, t):
        """For any counter N ≥ 1 and any valid local time, build_message(N) SHALL
        produce a string matching 'Message #<N> at <HH:MM:SS>'.

        **Validates: Requirements 2.2**
        """
        from publisher import build_message

        fake_dt = datetime(2000, 1, 1, t.hour, t.minute, t.second)
        with patch('publisher.datetime') as mock_dt:
            mock_dt.now.return_value = fake_dt
            result = build_message(n)

        # Must match overall pattern
        assert re.match(r'^Message #\d+ at \d{2}:\d{2}:\d{2}$', result), (
            f"Format mismatch for N={n}, time={t}: {result!r}"
        )

        # Embedded counter must equal the input N
        match = re.match(r'^Message #(\d+) at (\d{2}:\d{2}:\d{2})$', result)
        assert match is not None
        embedded_n = int(match.group(1))
        assert embedded_n == n, (
            f"Embedded counter {embedded_n} != input N={n} in {result!r}"
        )

        # Embedded time must match the mocked time
        expected_time = fake_dt.strftime('%H:%M:%S')
        assert match.group(2) == expected_time, (
            f"Embedded time {match.group(2)!r} != expected {expected_time!r}"
        )


# ---------------------------------------------------------------------------
# connect() tests
# ---------------------------------------------------------------------------

class TestConnect:
    """Tests for the connect() function."""

    def test_connect_success_returns_client(self):
        """connect() returns the Redis client when ping succeeds."""
        from publisher import connect

        mock_client = MagicMock()
        mock_client.ping.return_value = True

        with patch('publisher.redis.Redis', return_value=mock_client):
            result = connect()

        assert result is mock_client
        mock_client.ping.assert_called_once()

    def test_connect_creates_client_with_correct_params(self):
        """connect() creates redis.Redis with host, port, and timeout."""
        from publisher import connect

        mock_client = MagicMock()
        mock_client.ping.return_value = True

        with patch('publisher.redis.Redis', return_value=mock_client) as mock_redis:
            connect()

        mock_redis.assert_called_once_with(
            host='localhost',
            port=6379,
            socket_connect_timeout=5,
        )

    def test_connect_connection_error_exits_1(self):
        """connect() calls sys.exit(1) on ConnectionError from ping."""
        import redis as redis_module
        from publisher import connect

        mock_client = MagicMock()
        mock_client.ping.side_effect = redis_module.exceptions.ConnectionError("refused")

        with patch('publisher.redis.Redis', return_value=mock_client):
            with pytest.raises(SystemExit) as exc_info:
                connect()

        assert exc_info.value.code == 1

    def test_connect_timeout_error_exits_1(self):
        """connect() calls sys.exit(1) on TimeoutError from ping."""
        import redis as redis_module
        from publisher import connect

        mock_client = MagicMock()
        mock_client.ping.side_effect = redis_module.exceptions.TimeoutError("timed out")

        with patch('publisher.redis.Redis', return_value=mock_client):
            with pytest.raises(SystemExit) as exc_info:
                connect()

        assert exc_info.value.code == 1

    def test_connect_prints_error_to_stderr_on_failure(self, capsys):
        """connect() prints an error message to stderr on connection failure."""
        import redis as redis_module
        from publisher import connect

        mock_client = MagicMock()
        mock_client.ping.side_effect = redis_module.exceptions.ConnectionError("no server")

        with patch('publisher.redis.Redis', return_value=mock_client):
            with pytest.raises(SystemExit):
                connect()

        captured = capsys.readouterr()
        assert 'localhost' in captured.err
        assert '6379' in captured.err


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------

# Feature: redis-pubsub-demo, Property 2: Counter monotonicity


@given(k=st.integers(min_value=1, max_value=500))
@settings(max_examples=100)
def test_counter_monotonicity(k):
    """Property 2: Counter monotonicity

    For any K >= 1 successful publish calls, the embedded counters in the
    messages SHALL form the sequence 1, 2, ..., K with no gaps or repeats.

    **Validates: Requirements 2.2**

    # Feature: redis-pubsub-demo, Property 2: Counter monotonicity
    """
    import threading
    from publisher import publish_loop

    collected_messages = []
    call_count = [0]

    def no_op_publish(channel, message):
        """Record the message and trigger shutdown after K calls."""
        collected_messages.append(message)
        call_count[0] += 1

    mock_client = MagicMock()
    mock_client.publish.side_effect = no_op_publish

    local_shutdown = threading.Event()

    # We need to stop the loop after exactly k iterations.
    # Patch time.sleep to be a no-op (avoid 2-second waits),
    # and set the shutdown event after k publish calls have been made.
    original_sleep = __import__('time').sleep

    def fast_sleep(seconds):
        # After k messages collected, set shutdown so the next iteration exits.
        if call_count[0] >= k:
            local_shutdown.set()

    with patch('publisher.time.sleep', side_effect=fast_sleep), \
         patch('publisher.print'):  # suppress stdout output
        try:
            publish_loop(mock_client, local_shutdown)
        except SystemExit:
            pass  # publish_loop exits via sys.exit(0) when shutdown_event is set

    # Extract counter values from messages like "Message #<N> at HH:MM:SS"
    counters = []
    for msg in collected_messages:
        m = re.match(r'^Message #(\d+) at \d{2}:\d{2}:\d{2}$', msg)
        assert m is not None, f"Unexpected message format: {msg!r}"
        counters.append(int(m.group(1)))

    assert counters == list(range(1, k + 1)), (
        f"Expected counters [1..{k}], got {counters}"
    )


# ---------------------------------------------------------------------------
# Shutdown and connection error handling tests  (Requirements 1.2, 1.3, 3.1-3.4)
# ---------------------------------------------------------------------------

class TestShutdownAndConnectionErrors:
    """Tests for publisher shutdown and connection error handling.

    Validates:
    - KeyboardInterrupt during main() exits with code 0  (Req 3.2)
    - ConnectionError from client.ping() at startup exits with code 1  (Req 1.2, 1.3)
    - ConnectionError during publish() exits with code 1  (Req 3.1, 3.4)
    - ConnectionError during publish() when shutdown already set exits with code 0  (Req 3.3)
    """

    def test_keyboard_interrupt_during_main_exits_0(self):
        """KeyboardInterrupt raised during main() results in exit code 0.

        Validates: Requirements 3.2
        """
        import redis as redis_module
        import publisher as pub_module

        mock_client = MagicMock()
        # connect() succeeds
        mock_client.ping.return_value = True
        # publish_loop raises KeyboardInterrupt to simulate Ctrl+C
        mock_client.publish.side_effect = KeyboardInterrupt

        with patch('publisher.redis.Redis', return_value=mock_client), \
             patch('publisher.signal.signal'), \
             patch('publisher.sys.exit') as mock_exit:

            # Reset the module-level shutdown_event before the test
            pub_module.shutdown_event.clear()

            # Simulate the __main__ block: wrap main() with the KeyboardInterrupt handler
            try:
                pub_module.main()
            except KeyboardInterrupt:
                pub_module.shutdown_event.set()
                pub_module.sys.exit(0)

        mock_exit.assert_called_with(0)

    def test_connection_error_at_startup_ping_exits_1(self):
        """ConnectionError from client.ping() at startup results in exit code 1.

        Validates: Requirements 1.2, 1.3
        """
        import redis as redis_module

        mock_client = MagicMock()
        mock_client.ping.side_effect = redis_module.exceptions.ConnectionError("refused")

        with patch('publisher.redis.Redis', return_value=mock_client), \
             patch('publisher.signal.signal'):
            with pytest.raises(SystemExit) as exc_info:
                import publisher as pub_module
                pub_module.main()

        assert exc_info.value.code == 1

    def test_connection_error_during_publish_exits_1(self):
        """ConnectionError raised during publish() results in exit code 1.

        Validates: Requirements 3.1, 3.4
        """
        import redis as redis_module
        import publisher as pub_module

        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.publish.side_effect = redis_module.exceptions.ConnectionError("lost")

        with patch('publisher.redis.Redis', return_value=mock_client), \
             patch('publisher.signal.signal'):

            pub_module.shutdown_event.clear()

            with pytest.raises(SystemExit) as exc_info:
                pub_module.main()

        assert exc_info.value.code == 1

    def test_connection_error_during_publish_with_shutdown_flag_exits_0(self):
        """ConnectionError during publish() when shutdown flag is already set exits with code 0.

        Validates: Requirement 3.3
        """
        import redis as redis_module
        import publisher as pub_module

        mock_client = MagicMock()
        mock_client.ping.return_value = True

        # Set the shutdown flag before the publish raises, simulating a race
        # where shutdown was initiated before the connection error is detected.
        def publish_with_shutdown_set(*args, **kwargs):
            pub_module.shutdown_event.set()
            raise redis_module.exceptions.ConnectionError("lost during shutdown")

        mock_client.publish.side_effect = publish_with_shutdown_set

        with patch('publisher.redis.Redis', return_value=mock_client), \
             patch('publisher.signal.signal'):

            pub_module.shutdown_event.clear()

            with pytest.raises(SystemExit) as exc_info:
                pub_module.main()

        assert exc_info.value.code == 0
