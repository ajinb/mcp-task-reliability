import json

import pytest

from mcp_task_reliability.cli import main


@pytest.mark.parametrize("argv", [[], ["demo"], ["crash"], ["poll"], ["retry"]])
def test_commands_exit_zero_and_print_something(argv, capsys):
    assert main(argv) == 0
    assert capsys.readouterr().out.strip()


@pytest.mark.parametrize("argv", [["demo"], ["crash"], ["poll"], ["retry"]])
def test_json_output_parses(argv, capsys):
    assert main(argv + ["--json"]) == 0
    assert json.loads(capsys.readouterr().out)


def test_demo_json_has_all_three_sections(capsys):
    main(["demo", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"crash_recovery", "poll_storm", "retry_classes"}
    assert len(payload["crash_recovery"]) == 2
    assert len(payload["retry_classes"]) == 3


def test_unknown_command_is_rejected():
    with pytest.raises(SystemExit):
        main(["nonsense"])
