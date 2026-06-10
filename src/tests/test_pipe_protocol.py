import base64

import pytest

from src.worker.pipe_protocol import (
    CheckpointMessage,
    ResultMessage,
    build_checkpoint_message,
    build_result_message,
    parse_pipe_message,
)


class TestBuildCheckpointMessage:
    def test_returns_checkpoint_type(self):
        msg = build_checkpoint_message(sequence=1, data=b"hello")
        assert msg["type"] == "checkpoint"

    def test_sequence_preserved(self):
        msg = build_checkpoint_message(sequence=7, data=b"x")
        assert msg["sequence"] == 7

    def test_data_roundtrips_via_base64(self):
        original = b"\x00\xff\xfe\xab"
        msg = build_checkpoint_message(sequence=1, data=original)
        assert base64.b64decode(msg["data"]) == original

    def test_empty_data_encodes(self):
        msg = build_checkpoint_message(sequence=0, data=b"")
        assert base64.b64decode(msg["data"]) == b""


class TestBuildResultMessage:
    def test_success_result(self):
        msg = build_result_message(success=True, result={"x": 42})
        assert msg["type"] == "result"
        assert msg["success"] is True
        assert msg["result"] == {"x": 42}
        assert msg["error"] is None
        assert msg["traceback"] is None

    def test_failure_result(self):
        msg = build_result_message(
            success=False, error="oops", traceback_str="Traceback..."
        )
        assert msg["type"] == "result"
        assert msg["success"] is False
        assert msg["error"] == "oops"
        assert msg["traceback"] == "Traceback..."

    def test_defaults_to_none_for_optional_fields(self):
        msg = build_result_message(success=True)
        assert msg["result"] is None
        assert msg["error"] is None
        assert msg["traceback"] is None


class TestParsePipeMessage:
    def test_parses_checkpoint(self):
        raw = build_checkpoint_message(sequence=3, data=b"state")
        parsed = parse_pipe_message(raw)
        assert isinstance(parsed, CheckpointMessage)
        assert parsed.sequence == 3
        assert parsed.data == b"state"

    def test_parses_result_success(self):
        raw = build_result_message(success=True, result={"done": True})
        parsed = parse_pipe_message(raw)
        assert isinstance(parsed, ResultMessage)
        assert parsed.success is True
        assert parsed.result == {"done": True}

    def test_parses_result_failure(self):
        raw = build_result_message(success=False, error="bad", traceback_str="tb")
        parsed = parse_pipe_message(raw)
        assert isinstance(parsed, ResultMessage)
        assert parsed.success is False
        assert parsed.error == "bad"
        assert parsed.traceback == "tb"

    def test_raises_on_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown message type"):
            parse_pipe_message({"type": "garbage"})

    def test_raises_on_missing_type(self):
        with pytest.raises(ValueError, match="Unknown message type"):
            parse_pipe_message({})
