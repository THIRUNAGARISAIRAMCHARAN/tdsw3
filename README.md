# Q2 - A2A Invoice Agent

Public HTTPS A2A 1.0 agent (HTTP+JSON). Starlette + SQLite, one model call per new batch (cached by package content).

## Files
- `a2a_app.py` - routes, auth, versions, media types, idempotency, task lifecycle, user isolation, cancel/result race
- `store.py`   - SQLite persistence (tasks, message idempotency, decision cache, executions, debug log)
- `decide.py`  - one batched AI call (AI Pipe), strict validation of every proposal, content-keyed cache
- `main.py`, `requirements.txt`, `Dockerfile` - deployment
- `test_a2a.py` - 53 local protocol checks (`python3 test_a2a.py`)
- `smoke.py`    - end-to-end check of a deployed URL without using a grader check (`python3 smoke.py https://HOST/a2a`)

## Deploy (Render, Docker, Free instance, ONE instance)
Environment variables:
| name | value |
|---|---|
| `AIPIPE_TOKEN` | your AI Pipe token |
| `DEBUG_KEY` | any private string (enables `/_debug/list` and `/_debug/get/<id>`, header `X-Debug-Key`) |
| `MODEL` | optional, default `gpt-4.1-mini` |
| `REF_BRACKETS` | optional, `strip` (default) or `keep` - whether evidenceRefs keep their [brackets] |

Submit the base URL: `https://<your-service>.onrender.com/a2a/`

## Notes
- Any non-empty Bearer token is a separate user.
- State lives in SQLite on the instance's disk; keep the service awake between your Checks and the final Save.
