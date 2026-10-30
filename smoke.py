"""Smoke test against a DEPLOYED agent (uses 2 fake packages + the real AI call; costs a fraction of a cent).
Usage:  python3 smoke.py https://your-host/a2a
It does NOT use one of your 10 grader checks."""
import json, sys, urllib.request, urllib.error, uuid

base = sys.argv[1].rstrip("/")
origin = base.split("/", 3)[0] + "//" + base.split("/", 3)[2]
H = {"Authorization": "Bearer smoke-" + uuid.uuid4().hex[:8], "A2A-Version": "1.0", "Content-Type": "application/a2a+json", "Accept": "application/a2a+json"}

def call(method, url, headers=None, body=None):
    req = urllib.request.Request(url, method=method, headers=headers or {}, data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, dict(r.headers), json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), (json.loads(e.read() or b"null") if e.headers.get("content-type", "").startswith("application/") else None)

s, h, card = call("GET", origin + "/.well-known/agent-card.json")
print("1 card:", s, "| interface urls:", [i["url"] for i in card["supportedInterfaces"]])
print("2 no auth:", call("GET", base + "/tasks", {"A2A-Version": "1.0"})[0], "(want 401)")
print("3 bad version:", call("GET", base + "/tasks", {**H, "A2A-Version": "0.3"})[0], "(want 400)")
pk = [
 {"packageId": "S-1", "documents": [
   {"title": "Cover sheet", "text": "Cover ref [COVER-1]. Policy revision rev-1."},
   {"title": "Invoice", "text": "Vendor Northwind Traders, invoice NW-2041, total 1,250.00 USD, matches PO and receipt. Within the 5,000.00 USD autonomous limit. Decisive: [INV-NW-2041] [PO-8812] [GRN-5530]."}]},
 {"packageId": "S-2", "documents": [
   {"title": "Ledger", "text": "Invoice AC-77 from Acme Ltd for 9,999.00 USD was PAID on 2026-03-02. The same invoice AC-77 was received again. Decisive: [LEDGER-AC-77] [PAY-3301] [INV-AC-77-DUP]."}]},
]
msg = {"message": {"messageId": "smoke-1", "role": "ROLE_USER", "parts": [{"mediaType": "application/vnd.ga5.invoice-claim-batch+json",
       "data": {"batchId": "smoke-b1", "policyRevision": "rev-1", "packages": pk}}]},
       "configuration": {"returnImmediately": False, "historyLength": 20}}
s, h, js = call("POST", base + "/message:send", H, msg)
print("4 send:", s, h.get("Content-Type"))
task = js["task"]; print("  state:", task["status"]["state"])
for p in task["artifacts"][0]["parts"][0]["data"]["proposals"]:
    print("  ", p["packageId"], p["action"], p["facts"], p["evidenceRefs"]); print("     ", p["rationale"])
s2, _, js2 = call("POST", base + "/message:send", H, msg); print("5 replay same task:", s2 == 200 and js2["task"]["id"] == task["id"])
res = {"message": {"messageId": "smoke-2", "taskId": task["id"], "contextId": task["contextId"], "role": "ROLE_USER",
       "parts": [{"mediaType": "application/vnd.ga5.invoice-action-results+json", "data": {"batchId": "smoke-b1", "results": [
           {"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"], "outcome": "ACCEPTED", "receiptNonce": "n-" + p["packageId"]}
           for p in task["artifacts"][0]["parts"][0]["data"]["proposals"]]}}]}}
s, _, js = call("POST", base + "/message:send", H, res)
print("6 results:", s, js["task"]["status"]["state"], "| executions:", len(js["task"]["artifacts"][1]["parts"][0]["data"]["executions"]))
print("7 outsider read:", call("GET", base + "/tasks/" + task["id"], {**H, "Authorization": "Bearer someone-else"})[0], "(want 404)")
