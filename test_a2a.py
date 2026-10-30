"""Protocol tests with a stub decider (no network).  Run: python3 test_a2a.py"""
import asyncio, json, os, tempfile, uuid
import a2a_app, decide as D
from store import Store

H = {"authorization": "Bearer tokenA", "a2a-version": "1.0", "content-type": "application/a2a+json", "accept": "application/a2a+json"}
HB = {**H, "authorization": "Bearer tokenB"}
P = "/a2a"
ACTIONS = D.ACTIONS

class StubDecider:
    def __init__(self): self.calls = 0; self.pkgs = 0
    async def decide(self, header, packages):
        self.calls += 1; self.pkgs += len(packages)
        await asyncio.sleep(0.05)
        out = {}
        for i, p in enumerate(packages):
            out[D.pkg_id(p)] = {"action": ACTIONS[i % 5],
                "facts": {"vendorName": f"Vendor {i}", "invoiceNumber": f"INV-{i}", "amountMinor": 12345 + i, "currency": "INR"},
                "evidenceRefs": [f"R{i}-1", f"R{i}-2", f"R{i}-3"],
                "rationale": f"{ACTIONS[i % 5]} because R{i}-1 and R{i}-2 decide it under the current policy revision."}
        return out

async def call(app, method, path, headers=None, body=None, raw=None, query=""):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else b"")
    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": method, "path": path,
             "raw_path": path.encode(), "query_string": query.encode(), "scheme": "https", "root_path": "",
             "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
             "client": ("1.1.1.1", 1), "server": ("t", 443)}
    sent, got = [], [False]
    async def receive():
        if not got[0]:
            got[0] = True; return {"type": "http.request", "body": data, "more_body": False}
        await asyncio.sleep(3600)
    async def send(m): sent.append(m)
    await app(scope, receive, send)
    st = next(m for m in sent if m["type"] == "http.response.start")
    b = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    hd = {k.decode().lower(): v.decode() for k, v in st["headers"]}
    try: js = json.loads(b) if b else None
    except ValueError: js = None
    return st["status"], hd, js, b

def batch_msg(mid, batch="B1", n=12, ctx=None, cfg=None):
    pk = [{"packageId": f"{batch}-p{i}", "documents": [{"title": "d", "text": f"para [R{i}-1] [R{i}-2] [R{i}-3]"}]} for i in range(n)]
    m = {"messageId": mid, "role": "ROLE_USER", "parts": [{"mediaType": D.__dict__.get("X", a2a_app.BATCH_MT),
         "data": {"batchId": batch, "policyRevision": "rev-9", "packages": pk}}]}
    if ctx: m["contextId"] = ctx
    return {"message": m, "configuration": cfg or {"returnImmediately": False, "historyLength": 20}}

def results_msg(task, mid, outcomes=None, **over):
    prop = task["artifacts"][0]["parts"][0]["data"]
    res = []
    for i, p in enumerate(prop["proposals"]):
        oc = (outcomes or {}).get(i, "ACCEPTED")
        res.append({"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"], "outcome": oc, "receiptNonce": f"nonce-{i}-{uuid.uuid4().hex[:6]}"})
    data = {"batchId": prop["batchId"], "results": res}
    data.update(over.pop("data", {}))
    m = {"messageId": mid, "taskId": task["id"], "contextId": task["contextId"], "role": "ROLE_USER",
         "parts": [{"mediaType": a2a_app.RESULTS_MT, "data": data}]}
    m.update(over)
    return {"message": m}

fails = 0
def check(name, cond, extra=""):
    global fails
    fails += 0 if cond else 1
    print("PASS" if cond else "FAIL", name, "" if cond else str(extra)[:300])

async def main():
    d = StubDecider()
    store = Store(os.path.join(tempfile.mkdtemp(), "t.db"))
    app = a2a_app.build_app(store, d)
    # ---- card / auth / version / media type
    s, h, js, _ = await call(app, "GET", "/.well-known/agent-card.json", {"host": "x.example"})
    check("card public 200", s == 200 and js["name"] and js["description"] and js["version"] and isinstance(js["capabilities"], dict), js)
    check("card interface has exact base", {"url": "https://x.example/a2a", "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"} in js["supportedInterfaces"], js["supportedInterfaces"])
    check("card modes", "application/vnd.ga5.invoice-claim-batch+json" in js["defaultInputModes"] and set(js["defaultOutputModes"]) >= {a2a_app.PROPOSALS_MT, a2a_app.RECEIPTS_MT})
    sk = js["skills"][0]
    check("card skill", sk["id"] == "invoice_action_agent" and sk["name"] and sk["description"] and sk["tags"])
    s, *_ = await call(app, "GET", "/a2a/tasks", {"a2a-version": "1.0"}); check("no auth -> 401", s == 401)
    s, *_ = await call(app, "GET", "/a2a/tasks", {**H, "authorization": "Basic abc"}); check("wrong scheme -> 401", s == 401)
    s, *_ = await call(app, "GET", "/a2a/tasks", {**H, "a2a-version": "0.3"}); check("version 0.3 -> 400", s == 400)
    s, *_ = await call(app, "POST", "/a2a/message:send", {**H, "content-type": "text/plain"}, raw=b"{}"); check("bad content-type -> 415", s == 415)
    s, *_ = await call(app, "POST", "/a2a/message:send", H, raw=b"{not json"); check("bad json -> 400", s == 400)
    s, *_ = await call(app, "POST", "/a2a/message:send", H, body={"message": {"messageId": "x"}}); check("bad envelope -> 400", s == 400)
    s, *_ = await call(app, "POST", "/a2a//message:send", H, body={}); check("double slash tolerated (400 not 404)", s == 400)
    # ---- initial message
    s, h, js, raw = await call(app, "POST", "/a2a/message:send", H, body=batch_msg("m1"))
    task = js["task"]
    check("initial 200 + a2a media type", s == 200 and h["content-type"].startswith("application/a2a+json"), (s, h))
    check("state INPUT_REQUIRED", task["status"]["state"] == "TASK_STATE_INPUT_REQUIRED", task["status"])
    arts = task["artifacts"]
    check("exactly one artifact with one proposals part", len(arts) == 1 and len(arts[0]["parts"]) == 1 and arts[0]["parts"][0]["mediaType"] == a2a_app.PROPOSALS_MT)
    props = arts[0]["parts"][0]["data"]["proposals"]
    check("12 proposals, unique ids >=12 chars, typed fields", len(props) == 12 and len({p["actionId"] for p in props}) == 12 and all(len(p["actionId"]) >= 12 and p["action"] in ACTIONS and 60 <= len(p["rationale"]) <= 1500 for p in props))
    check("history holds initial message", len(task["history"]) == 1 and task["history"][0]["messageId"] == "m1" and task["history"][0]["taskId"] == task["id"])
    check("one model call", d.calls == 1)
    # ---- idempotency
    msg2 = json.loads(json.dumps(batch_msg("m1")))
    msg2["message"]["parts"][0]["data"] = dict(reversed(list(msg2["message"]["parts"][0]["data"].items())))  # reordered keys
    msg2["message"] = dict(reversed(list(msg2["message"].items())))
    msg2["configuration"] = {"returnImmediately": True}
    s, _, js2, _ = await call(app, "POST", "/a2a/message:send", H, body=msg2)
    check("replay (reordered keys, other returnImmediately) -> same task", s == 200 and js2["task"] == task, js2)
    check("replay made no model call", d.calls == 1)
    changed = batch_msg("m1"); changed["message"]["parts"][0]["data"]["policyRevision"] = "rev-10"
    s, _, js3, _ = await call(app, "POST", "/a2a/message:send", H, body=changed)
    check("changed content same messageId -> 409 IDEMPOTENCY_CONFLICT", s == 409 and "IDEMPOTENCY_CONFLICT" in json.dumps(js3), js3)
    s, _, got, _ = await call(app, "GET", f"/a2a/tasks/{task['id']}", H)
    check("conflict left original unchanged", got == task)
    # concurrent equivalent requests -> one model call, same task
    d2 = StubDecider(); app2 = a2a_app.build_app(Store(os.path.join(tempfile.mkdtemp(), "c.db")), d2)
    rs = await asyncio.gather(*[call(app2, "POST", "/a2a/message:send", H, body=batch_msg("cm", "C1")) for _ in range(8)])
    ids = {r[2]["task"]["id"] for r in rs}
    check("8 concurrent equivalent sends -> 1 task, 1 model call", len(ids) == 1 and d2.calls == 1 and all(r[0] == 200 for r in rs), (ids, d2.calls))
    # ---- polls / lists make no model call
    calls_before = d.calls
    for _ in range(3):
        await call(app, "GET", f"/a2a/tasks/{task['id']}", H); await call(app, "GET", "/a2a/tasks", H)
    check("polls and lists make no model call", d.calls == calls_before)
    s, _, lst, _ = await call(app, "GET", "/a2a/tasks", H)
    check("list has own task, no history", s == 200 and [t["id"] for t in lst["tasks"]] == [task["id"]] and "history" not in lst["tasks"][0], lst)
    # ---- isolation
    s, _, js, _ = await call(app, "GET", f"/a2a/tasks/{task['id']}", HB); check("outsider GET -> 404 generic", s == 404 and task["id"] not in json.dumps(js), js)
    s, _, js, _ = await call(app, "POST", f"/a2a/tasks/{task['id']}:cancel", HB); check("outsider cancel -> 404", s == 404 and task["id"] not in json.dumps(js))
    s, _, js, _ = await call(app, "POST", "/a2a/message:send", HB, body=results_msg(task, "x-out")); check("outsider continuation -> 404", s == 404 and task["id"] not in json.dumps(js))
    s, _, lst, _ = await call(app, "GET", "/a2a/tasks", HB); check("outsider list empty", s == 200 and lst["tasks"] == [])
    s, _, got, _ = await call(app, "GET", f"/a2a/tasks/{task['id']}", H); check("task untouched by outsider", got == task)
    # same messageId used by another principal is independent
    s, _, jb, _ = await call(app, "POST", "/a2a/message:send", HB, body=batch_msg("m1", "B1"))
    check("same messageId, other principal -> own task", s == 200 and jb["task"]["id"] != task["id"])
    # ---- bad continuations leave the task untouched
    bads = {
        "wrong actionId": lambda r: r["message"]["parts"][0]["data"]["results"][0].update(actionId="act-forged-000000"),
        "wrong action": lambda r: r["message"]["parts"][0]["data"]["results"][0].update(action="settle_invoice" if r["message"]["parts"][0]["data"]["results"][0]["action"] != "settle_invoice" else "hold_invoice"),
        "unknown package": lambda r: r["message"]["parts"][0]["data"]["results"][0].update(packageId="nope"),
        "wrong batch": lambda r: r["message"]["parts"][0]["data"].update(batchId="OTHER"),
        "wrong context": lambda r: r["message"].update(contextId="other-ctx"),
        "bad outcome": lambda r: r["message"]["parts"][0]["data"]["results"][0].update(outcome="MAYBE"),
        "accepted without nonce": lambda r: r["message"]["parts"][0]["data"]["results"][0].pop("receiptNonce"),
    }
    for name, mut in bads.items():
        r = results_msg(task, "bad-" + name.replace(" ", "")); mut(r)
        s, _, js, _ = await call(app, "POST", "/a2a/message:send", H, body=r)
        s2, _, got, _ = await call(app, "GET", f"/a2a/tasks/{task['id']}", H)
        check(f"reject continuation: {name}", 400 <= s < 500 and got == task, (s, js))
    # ---- valid continuation
    r = results_msg(task, "res1", outcomes={1: "REJECTED", 7: "REJECTED"})
    s, _, js, _ = await call(app, "POST", "/a2a/message:send", H, body=r)
    done = js["task"]
    check("valid continuation -> COMPLETED", s == 200 and done["status"]["state"] == "TASK_STATE_COMPLETED", js)
    rec = [p for a in done["artifacts"] for p in a["parts"] if p["mediaType"] == a2a_app.RECEIPTS_MT]
    prop_part = [p for a in done["artifacts"] for p in a["parts"] if p["mediaType"] == a2a_app.PROPOSALS_MT]
    check("proposal artifact kept + exactly one receipts part", len(rec) == 1 and len(prop_part) == 1)
    ex = rec[0]["data"]["executions"]; pmap = {p["packageId"]: p for p in props}
    nonces = {x["packageId"]: x["receiptNonce"] for x in r["message"]["parts"][0]["data"]["results"]}
    check("only ACCEPTED executed (10 of 12)", len(ex) == 10 and props[1]["packageId"] not in {e["packageId"] for e in ex} and props[7]["packageId"] not in {e["packageId"] for e in ex})
    check("executions match proposal + grader nonce", all(e["facts"] == pmap[e["packageId"]]["facts"] and e["actionId"] == pmap[e["packageId"]]["actionId"] and e["action"] == pmap[e["packageId"]]["action"] and e["evidenceRefs"] == pmap[e["packageId"]]["evidenceRefs"] and e["receiptNonce"] == nonces[e["packageId"]] for e in ex))
    check("continuation recorded in history", [m["messageId"] for m in done["history"]] == ["m1", "res1"])
    # ---- terminal behaviour
    s, _, js, _ = await call(app, "POST", "/a2a/message:send", H, body=r)
    check("replay of same continuation -> same completed task", s == 200 and js["task"] == done)
    r2 = results_msg(task, "res2")
    s, _, js, _ = await call(app, "POST", "/a2a/message:send", H, body=r2); check("second continuation (new id) -> 409", s == 409, js)
    s, _, js, _ = await call(app, "POST", f"/a2a/tasks/{task['id']}:cancel", H); check("cancel after completion -> 409", s == 409)
    s, _, got, _ = await call(app, "GET", f"/a2a/tasks/{task['id']}", H); check("terminal task never changes", got == done)
    # ---- cancel then results
    s, _, js, _ = await call(app, "POST", "/a2a/message:send", H, body=batch_msg("m-c", "C2"))
    t2 = js["task"]
    s, _, js, _ = await call(app, "POST", f"/a2a/tasks/{t2['id']}:cancel", H)
    check("cancel -> CANCELED, no receipts", s == 200 and js["status"]["state"] == "TASK_STATE_CANCELED" and not any(p["mediaType"] == a2a_app.RECEIPTS_MT for a in js["artifacts"] for p in a["parts"]), js)
    s, _, js, _ = await call(app, "POST", "/a2a/message:send", H, body=results_msg(t2, "late")); check("results after cancel -> 409", s == 409)
    # ---- race: cancel vs results, repeated
    clean = True
    for i in range(10):
        s, _, js, _ = await call(app, "POST", "/a2a/message:send", H, body=batch_msg(f"race{i}", f"R{i}"))
        t3 = js["task"]
        a, b = await asyncio.gather(call(app, "POST", "/a2a/message:send", H, body=results_msg(t3, f"rr{i}")),
                                    call(app, "POST", f"/a2a/tasks/{t3['id']}:cancel", H))
        codes = sorted([a[0], b[0]])
        s, _, fin, _ = await call(app, "GET", f"/a2a/tasks/{t3['id']}", H)
        has_rec = any(p["mediaType"] == a2a_app.RECEIPTS_MT for x in fin["artifacts"] for p in x["parts"])
        st = fin["status"]["state"]
        if codes != [200, 409] or (st == "TASK_STATE_COMPLETED") != has_rec or st not in ("TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"):
            clean = False; print("  race", i, codes, st, has_rec)
    check("10 cancel-vs-result races: exactly one 200 + one 409, never both outcomes", clean)
    # ---- cache: same content under new namespace -> no new model packages
    real_store = Store(os.path.join(tempfile.mkdtemp(), "cache.db"))
    class Counting(D.Decider):
        n = 0
    dd = D.Decider(real_store, "tok")
    calls = []
    def fake_llm(header, pk, token, model, timeout):
        calls.append(len(pk))
        return {"decisions": [{"packageId": D.pkg_id(p), "analysis": "x", "action": "hold_invoice",
                               "facts": {"vendorName": "V", "invoiceNumber": "I1", "amountMinor": 500, "currency": "usd"},
                               "evidenceRefs": ["[R0-1]", "[R0-2]", "[R0-3]"], "rationale": "short"} for p in pk]}
    D.call_llm = fake_llm
    pk1 = batch_msg("z", "NS1", n=3)["message"]["parts"][0]["data"]["packages"]
    pk2 = batch_msg("z", "NS2", n=3)["message"]["parts"][0]["data"]["packages"]   # same content, new ids
    out1 = await dd.decide({"batchId": "NS1"}, pk1)
    out2 = await dd.decide({"batchId": "NS2"}, pk2)
    check("same content, new batch namespace -> cached (1 model call total)", calls == [3] and len(out2) == 3, calls)
    dec = out1["NS1-p0"]
    check("decision repaired: currency upper, rationale 60+ chars w/ action and refs", dec["facts"]["currency"] == "USD" and len(dec["rationale"]) >= 60 and "hold_invoice" in dec["rationale"] and sum(r in dec["rationale"] for r in dec["evidenceRefs"]) >= 2, dec)
    # model returning garbage -> safe fallback, never settle
    D.call_llm = lambda *a, **k: (_ for _ in ()).throw(TimeoutError("down"))
    pk3 = [{"packageId": "Q1", "documents": [{"text": "unique [Z-1] [Z-2] [Z-3] text"}]}]
    out3 = await dd.decide({"batchId": "Q"}, pk3)
    check("model down -> fallback hold_invoice (never pays), not cached", out3["Q1"]["action"] == "hold_invoice" and real_store.get_decision(D.cache_key(pk3[0])) is None)
    # persistence across restart
    path = os.path.join(tempfile.mkdtemp(), "p.db"); sA = Store(path); aA = a2a_app.build_app(sA, StubDecider())
    s, _, js, _ = await call(aA, "POST", "/a2a/message:send", H, body=batch_msg("pm", "P1")); tp = js["task"]
    aB = a2a_app.build_app(Store(path), StubDecider())   # "restart"
    s, _, got, _ = await call(aB, "GET", f"/a2a/tasks/{tp['id']}", H)
    s2, _, js2, _ = await call(aB, "POST", "/a2a/message:send", H, body=batch_msg("pm", "P1"))
    check("state survives restart; retry returns same task without model call", got == tp and js2["task"] == tp)

asyncio.run(main())
print("\nall passed" if not fails else f"\n{fails} FAILED"); raise SystemExit(1 if fails else 0)
