# Contributing

Thanks for your interest in mcp-task-reliability.

- Run `pip install -e ".[dev]"`, then `pytest -q` and `ruff check src tests examples` before opening a PR.
- Every policy is a small, pure, injectable object. New retry classes go in `retry.py`, new retention
  rules in `policy.py`, new backends in `store.py` — each with a unit test alongside.
- **Nothing in the core may call `time.time()` or `time.sleep()` directly.** Take a `clock` and advance
  it in tests. Determinism is the whole point; a test that sleeps will be rejected.
- Stores must satisfy the shared contract suite in `tests/test_store.py::TestStoreContract`. Add your
  backend to the fixture params and it is tested for free.
- Keep the core dependency-free. Transport and SDK adapters are optional extras.
