"""A2A 1.0 HTTP+JSON invoice-action agent (Starlette).

Layers: protocol/auth (here)  |  persistence (store.py)  |  AI decisions (decide.py).
Only POST /message:send for a NEW batch ever reaches the model; polls, lists, replays, conflicts,
cancels and result continuations are pure storage operations.
"""
import asyncio
import copy
import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

import decide as D
from store import Store

BATCH_MT = "application/vnd.ga5.invoice-claim-batch+json"
RESULTS_MT = "application/vnd.ga5.invoice-action-results+json"
PROPOSALS_MT = "application/vnd.ga5.invoice-action-proposals+json"
RECEIPTS_MT = "application/vnd.ga5.invoice-action-receipts+json"
A2A_MT = "application/a2a+json"
TERMINAL = {"TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED", "TASK_STATE_REJECTED"}
MAX_BODY = 6 * 1024 * 1024
MAX_RESPONSE = 480 * 1024
STATUS_NAMES = {400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED",
                406: "NOT_ACCEPTABLE", 409: "ABORTED", 415: "UNSUPPORTED_MEDIA_TYPE", 500: "INTERNAL", 503: "UNAVAILABLE"}


class ApiError(Exception):
    def __init__(self, status, reason, message, headers=None):
        self.status, self.reason, self.message, self.headers = status, reason, message, headers or {}


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canon(o):
    return json.dumps(o, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def dumps(o):
    return json.dumps(o, separators=(",", ":"), ensure_ascii=False).encode()


def err_response(e: ApiError):
    body = {"error": {"code": e.status, "status": STATUS_NAMES.get(e.status, "ERROR"), "message": e.message,
                      "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": e.reason,
                                   "domain": "a2a-protocol.org"}]},
            "reason": e.reason}
    return Response(dumps(body), e.status, headers=e.headers, media_type="application/json")


def ok(obj, status=200):
    return Response(dumps(obj), status, media_type=A2A_MT)


def not_found():
    return ApiError(404, "TASK_NOT_FOUND", "Task not found")


def build_app(store=None, decider=None):
    store = store or Store(os.getenv("DB_PATH", "/tmp/a2a.db"))
    decider = decider or D.Decider(store, os.getenv("AIPIPE_TOKEN", ""), os.getenv("MODEL", "gpt-4.1-mini"))
    env_base = os.getenv("BASE_URL", "").strip()
    prefix = (urlparse(env_base).path.rstrip("/") if env_base else "") or "/a2a"
    debug_key = os.getenv("DEBUG_KEY", "")
    locks = {}

    # ------------------------------------------------------------ helpers
    def base_url(request):
        if env_base:
            return env_base.rstrip("/")
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme).split(",")[0].strip()
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
        return f"{proto}://{host}{prefix}"

    def agent_card(request):
        base = base_url(request)
        inputs = [BATCH_MT, RESULTS_MT]
        outputs = [PROPOSALS_MT, RECEIPTS_MT]
        return {
            "name": "Invoice Action Agent",
            "description": "Reads invoice packages, proposes one evidence-backed business action per invoice, "
                           "and records receipts only for actions the grader accepts.",
            "version": "1.0.0",
            "supportedInterfaces": [
                {"url": base, "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"},
                {"url": base + "/", "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"},
            ],
            "capabilities": {"streaming": False, "pushNotifications": False, "extendedAgentCard": False},
            "securitySchemes": {"bearer": {"httpAuthSecurityScheme": {"scheme": "Bearer"}}},
            "securityRequirements": [{"schemes": {"bearer": {"list": []}}}],
            "defaultInputModes": inputs,
            "defaultOutputModes": outputs,
            "skills": [{
                "id": "invoice_action_agent",
                "name": "Invoice action agent",
                "description": "Chooses settle, approve, hold, reject-duplicate or exception for each invoice "
                               "package and cites the decisive evidence.",
                "tags": ["invoice", "accounts-payable", "approval", "duplicate-detection"],
                "inputModes": inputs, "outputModes": outputs,
            }],
        }

    def authenticate(request):
        m = re.fullmatch(r"Bearer\s+(\S+)", request.headers.get("authorization", ""), re.I)
        if not m:
            raise ApiError(401, "UNAUTHENTICATED", "Authentication required", {"WWW-Authenticate": "Bearer"})
        return hashlib.sha256(m.group(1).encode()).hexdigest()

    def check_protocol(request, has_body):
        if request.headers.get("a2a-version") != "1.0":
            raise ApiError(400, "VERSION_NOT_SUPPORTED", "Unsupported A2A-Version; use 1.0")
        accept = request.headers.get("accept", "")
        if accept and not re.search(r"(application/a2a\+json|application/json|application/\*|\*/\*)", accept, re.I):
            raise ApiError(406, "NOT_ACCEPTABLE", "Supported response type: application/a2a+json")
        if has_body:
            ct = request.headers.get("content-type", "").split(";")[0].strip().lower()
            if ct not in (A2A_MT, "application/json"):
                raise ApiError(415, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/a2a+json")

    async def read_json(request):
        raw = await request.body()
        if len(raw) > MAX_BODY:
            raise ApiError(400, "INVALID_ARGUMENT", "Request too large")
        try:
            body = json.loads(raw or b"{}")
        except ValueError:
            raise ApiError(400, "INVALID_ARGUMENT", "Body is not valid JSON")
        if not isinstance(body, dict):
            raise ApiError(400, "INVALID_ARGUMENT", "Body must be a JSON object")
        return body

    def fit(task, history_length=None):
        t = copy.deepcopy(task)
        if history_length is not None:
            h = t.get("history") or []
            t["history"] = h[-history_length:] if history_length else []
        if len(dumps({"task": t})) > MAX_RESPONSE:
            t.pop("history", None)
        return t

    def task_state(task):
        return task["status"]["state"]

    # ------------------------------------------------------------ message:send
    def validate_message(body):
        msg = body.get("message")
        if not isinstance(msg, dict):
            raise ApiError(400, "INVALID_ARGUMENT", "message is required")
        mid = msg.get("messageId")
        if not isinstance(mid, str) or not mid.strip() or len(mid) > 256:
            raise ApiError(400, "INVALID_ARGUMENT", "messageId is required")
        if msg.get("role") != "ROLE_USER":
            raise ApiError(400, "INVALID_ARGUMENT", "role must be ROLE_USER")
        parts = msg.get("parts")
        if not isinstance(parts, list) or not parts or not all(isinstance(p, dict) for p in parts):
            raise ApiError(400, "INVALID_ARGUMENT", "parts are required")
        return msg, mid

    def find_part(msg, media_type):
        for p in msg["parts"]:
            if p.get("mediaType") == media_type:
                if not isinstance(p.get("data"), dict):
                    raise ApiError(400, "INVALID_ARGUMENT", "part data must be an object")
                return p["data"]
        raise ApiError(415, "UNSUPPORTED_MEDIA_TYPE", "No supported part in message")

    async def create_task(principal, msg, mid, mhash):
        data = find_part(msg, BATCH_MT)
        batch_id, packages = data.get("batchId"), data.get("packages")
        if not isinstance(batch_id, str) or not batch_id or not isinstance(packages, list) or not packages \
                or len(packages) > 200 or not all(isinstance(p, dict) and D.pkg_id(p) for p in packages):
            raise ApiError(400, "INVALID_ARGUMENT", "batchId and packages are required")
        ids = [D.pkg_id(p) for p in packages]
        if len(set(ids)) != len(ids):
            raise ApiError(400, "INVALID_ARGUMENT", "package ids must be unique")
        header = {k: v for k, v in data.items() if k != "packages"}
        store.debug("initial", {"message": msg}, keep=150)
        decisions = await decider.decide(header, packages)
        proposals = []
        for p in packages:
            d = decisions[D.pkg_id(p)]
            proposals.append({"packageId": D.pkg_id(p), "actionId": "act-" + uuid.uuid4().hex,
                              "action": d["action"], "facts": d["facts"],
                              "evidenceRefs": d["evidenceRefs"], "rationale": d["rationale"]})
        task_id, ctx = str(uuid.uuid4()), (msg.get("contextId") if isinstance(msg.get("contextId"), str) and msg.get("contextId") else str(uuid.uuid4()))
        hist = copy.deepcopy(msg)
        hist["taskId"], hist["contextId"] = task_id, ctx
        task = {
            "id": task_id, "contextId": ctx,
            "status": {"state": "TASK_STATE_INPUT_REQUIRED", "timestamp": now_iso()},
            "artifacts": [{"artifactId": "proposals-" + task_id, "name": "invoice-action-proposals",
                           "parts": [{"mediaType": PROPOSALS_MT, "data": {"batchId": batch_id, "proposals": proposals}}]}],
            "history": [hist],
            "metadata": {"batchId": batch_id},
        }
        with store.tx():  # state is stored BEFORE we respond
            store.put_task(task_id, principal, task_state(task), task)
            store.put_message(principal, mid, mhash, task_id)
        return task

    def apply_results(principal, msg, mid, mhash):
        rec = store.get_task(msg["taskId"]) if isinstance(msg.get("taskId"), str) else None
        if not rec or rec[0] != principal:
            raise not_found()
        task = rec[1]
        if task_state(task) in TERMINAL:
            raise ApiError(409, "TASK_TERMINAL", "Task is already finished")
        if msg.get("contextId") not in (None, task["contextId"]):
            raise ApiError(400, "INVALID_ARGUMENT", "Message does not match the task")
        data = find_part(msg, RESULTS_MT)
        stored = task["artifacts"][0]["parts"][0]["data"]
        if data.get("batchId") != stored["batchId"] or not isinstance(data.get("results"), list):
            raise ApiError(400, "INVALID_ARGUMENT", "Results do not match the task")
        by_pkg = {p["packageId"]: p for p in stored["proposals"]}
        seen, accepted = set(), []
        for r in data["results"]:
            if not isinstance(r, dict):
                raise ApiError(400, "INVALID_ARGUMENT", "Results do not match the task")
            p = by_pkg.get(r.get("packageId"))
            if (p is None or r["packageId"] in seen or r.get("actionId") != p["actionId"]
                    or r.get("action") != p["action"] or r.get("outcome") not in ("ACCEPTED", "REJECTED")):
                raise ApiError(400, "INVALID_ARGUMENT", "Results do not match the task")
            seen.add(r["packageId"])
            if r["outcome"] == "ACCEPTED":
                nonce = r.get("receiptNonce")
                if not isinstance(nonce, str) or not nonce:
                    raise ApiError(400, "INVALID_ARGUMENT", "Results do not match the task")
                accepted.append({"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"],
                                 "receiptNonce": nonce, "facts": p["facts"], "evidenceRefs": p["evidenceRefs"]})
        new = copy.deepcopy(task)
        hist = copy.deepcopy(msg)
        hist["taskId"], hist["contextId"] = task["id"], task["contextId"]
        new["history"].append(hist)
        new["artifacts"].append({"artifactId": "receipts-" + task["id"], "name": "invoice-action-receipts",
                                 "parts": [{"mediaType": RECEIPTS_MT,
                                            "data": {"batchId": stored["batchId"], "executions": accepted}}]})
        new["status"] = {"state": "TASK_STATE_COMPLETED", "timestamp": now_iso()}
        with store.tx():
            for e in accepted:  # each action id can execute at most once
                store.record_execution(e["actionId"], task["id"], principal)
            store.put_task(task["id"], principal, task_state(new), new)
            store.put_message(principal, mid, mhash, task["id"])
        return new

    async def message_send(request, principal):
        check_protocol(request, True)
        body = await read_json(request)
        msg, mid = validate_message(body)
        mhash = hashlib.sha256(canon(msg).encode()).hexdigest()
        key = (principal, mid)
        lock = locks.setdefault(key, asyncio.Lock())
        async with lock:
            rec = store.get_message(principal, mid)
            if rec:
                if rec["hash"] != mhash:
                    raise ApiError(409, "IDEMPOTENCY_CONFLICT", "messageId was already used with different content")
                got = store.get_task(rec["task_id"])
                return ok({"task": fit(got[1])})
            if msg.get("taskId"):
                task = apply_results(principal, msg, mid, mhash)
            else:
                task = await create_task(principal, msg, mid, mhash)
            return ok({"task": fit(task)})

    # ------------------------------------------------------------ tasks
    def get_task(request, principal, task_id):
        check_protocol(request, False)
        rec = store.get_task(task_id)
        if not rec or rec[0] != principal:
            raise not_found()
        hl = request.query_params.get("historyLength")
        try:
            hl = max(0, int(hl)) if hl is not None else None
        except ValueError:
            raise ApiError(400, "INVALID_ARGUMENT", "historyLength must be an integer")
        return Response(dumps(fit(rec[1], hl)), 200, media_type=A2A_MT)  # a read returns the bare Task

    def list_tasks(request, principal):
        check_protocol(request, False)
        q = request.query_params
        try:
            size = min(100, max(1, int(q.get("pageSize", 50))))
            offset = max(0, int(q.get("pageToken") or 0))
        except ValueError:
            raise ApiError(400, "INVALID_ARGUMENT", "invalid paging parameters")
        tasks = store.list_tasks(principal)
        if q.get("contextId"):
            tasks = [t for t in tasks if t["contextId"] == q["contextId"]]
        if q.get("status"):
            tasks = [t for t in tasks if task_state(t) == q["status"]]
        page, total_bytes = [], 0
        for t in tasks[offset:offset + size]:
            t = {k: v for k, v in t.items() if k != "history"}  # lists omit history (fetch a task for it)
            n = len(dumps(t))
            if page and total_bytes + n > MAX_RESPONSE:
                break
            page.append(t)
            total_bytes += n
        nxt = offset + len(page)
        return ok({"tasks": page, "nextPageToken": str(nxt) if nxt < len(tasks) else "",
                   "pageSize": size, "totalSize": len(tasks)})

    def cancel_task(request, principal, task_id):
        check_protocol(request, False)
        rec = store.get_task(task_id)
        if not rec or rec[0] != principal:
            raise not_found()
        task = rec[1]
        if task_state(task) in TERMINAL:
            raise ApiError(409, "TASK_NOT_CANCELABLE", "Task is already finished")
        new = copy.deepcopy(task)
        new["status"] = {"state": "TASK_STATE_CANCELED", "timestamp": now_iso()}
        with store.tx():
            store.put_task(task_id, principal, task_state(new), new)
        return Response(dumps(fit(new)), 200, media_type=A2A_MT)

    # ------------------------------------------------------------ debug (disabled unless DEBUG_KEY is set)
    def debug_route(request, path):
        if not debug_key or request.headers.get("x-debug-key") != debug_key:
            raise ApiError(404, "NOT_FOUND", "Not found")
        if path == "/_debug/list":
            rows = store.debug_list(request.query_params.get("kind"), 30)
            return Response(dumps([{"id": r[0], "ts": r[1], "kind": r[2], "bytes": r[3]} for r in rows]),
                            200, media_type="application/json")
        m = re.fullmatch(r"/_debug/get/(\d+)", path)
        if m:
            return Response(dumps(store.debug_get(int(m.group(1)))), 200, media_type="application/json")
        raise ApiError(404, "NOT_FOUND", "Not found")

    # ------------------------------------------------------------ dispatcher
    async def dispatch(request: Request):
        try:
            path = re.sub(r"/{2,}", "/", request.scope["path"])
            method = request.method
            if path == "/.well-known/agent-card.json":
                if method not in ("GET", "HEAD"):
                    raise ApiError(405, "METHOD_NOT_ALLOWED", "Method not allowed")
                return Response(dumps(agent_card(request)), 200, media_type="application/json",
                                headers={"Cache-Control": "no-store"})
            if path in ("/", "/healthz"):
                return Response(dumps({"status": "ok", "service": "invoice action agent"}), 200,
                                media_type="application/json")
            if path.startswith("/_debug/"):
                return debug_route(request, path)
            if path != prefix and not path.startswith(prefix + "/"):
                raise ApiError(404, "NOT_FOUND", "Not found")
            sub = path[len(prefix):] or "/"
            principal = authenticate(request)
            if sub == "/message:send":
                if method != "POST":
                    raise ApiError(405, "METHOD_NOT_ALLOWED", "Method not allowed")
                return await message_send(request, principal)
            if sub == "/tasks":
                if method != "GET":
                    raise ApiError(405, "METHOD_NOT_ALLOWED", "Method not allowed")
                return list_tasks(request, principal)
            m = re.fullmatch(r"/tasks/([^/:]+)(:cancel)?", sub)
            if m:
                if m.group(2):
                    if method != "POST":
                        raise ApiError(405, "METHOD_NOT_ALLOWED", "Method not allowed")
                    return cancel_task(request, principal, m.group(1))
                if method != "GET":
                    raise ApiError(405, "METHOD_NOT_ALLOWED", "Method not allowed")
                return get_task(request, principal, m.group(1))
            raise ApiError(404, "NOT_FOUND", "Not found")
        except ApiError as e:
            return err_response(e)
        except Exception as exc:  # noqa: BLE001 - never leak internals
            try:
                store.debug("server_error", {"error": repr(exc)[:500], "path": request.scope.get("path")})
            except Exception:  # noqa: BLE001
                pass
            return err_response(ApiError(500, "INTERNAL", "Internal error"))

    methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
    app = Starlette(routes=[Route("/{path:path}", dispatch, methods=methods)])
    app.state.store = store
    return app


app = None


def get_app():
    global app
    if app is None:
        app = build_app()
    return app
