from unittest.mock import mock_open, patch

import pytest

from src.ai_agent.backlog_orchestrator import BacklogOrchestrator


@pytest.fixture
def mock_orchestrator():
    """Returns an orchestrator instance pointing to a dummy file."""
    return BacklogOrchestrator(input_file_path="mock_ideas.txt")


@patch("ollama.list")
@patch("ollama.chat")
@patch("builtins.open", new_callable=mock_open, read_data="Test idea 1\nTest idea 2")
@patch("os.path.exists")
def test_orchestrator_full_run_mocked(
    mock_exists, mock_file, mock_chat, mock_list, mock_orchestrator
):
    """
    Simulates a full run without triggering Ollama or writing to disk.
    Verifies the integration logic between agents.
    """
    # Setup mocks
    mock_exists.return_value = True
    mock_list.return_value = {"models": ["llama3:latest"]}
    mock_chat.return_value = {"message": {"content": "| Feature A | 10 | 2.0 | 1.0 | 5 | 4.0 |"}}

    # Execute
    # We also mock the internal _save and _update methods to avoid actual disk writes
    with (
        patch.object(BacklogOrchestrator, "_save"),
        patch.object(BacklogOrchestrator, "_update_history_log"),
    ):
        result = mock_orchestrator.run()

    # Assertions
    assert result is True
    mock_chat.assert_called()  # Confirms AI was "consulted"
    assert mock_chat.call_count == 2  # 1 for Analyst, 1 for Architect


def test_orchestrator_missing_file_handled(mock_orchestrator):
    """
    Verifies that run() returns False if the input file is missing.
    No mocks needed here as we are testing the failure branch.
    """
    with patch("os.path.exists", return_value=False):
        result = mock_orchestrator.run()
        assert result is False
