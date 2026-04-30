"""Self-contained apartment monitor for Nahalat Yitzhak.
Reads secrets from env vars: GH_PAT, GMAIL_PWD, GMAIL_ADDR.
Run via: curl https://raw.githubusercontent.com/Roy-works/roy-apartment-monitor/main/scripts/cron_monitor.py | python3
"""
import json, os, sys, subprocess, smtplib, ssl, urllib.parse, urllib.request
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

GITHUB_PAT = os.environ.get("GH_PAT") or sys.exit("missing GH_PAT")
GMAIL_PWD  = os.environ.get("GMAIL_PWD") or sys.exit("missing GMAIL_PWD")
GMAIL_TO   = os.environ.get("GMAIL_ADDR") or sys.exit("missing GMAIL_ADDR")

REPO_URL = f"https://x-access-token:{GITHUB_PAT}@github.com/Roy-works/roy-apartment-monitor.git"
PAGE_URL = "https://roy-works.github.io/roy-apartment-monitor/"
PARAMS = {"region":"3","area":"1","city":"5000","neighborhood":"317",
          "minRooms":"3","maxRooms":"4","minPrice":"8000","maxPrice":"12000","shelter":"1"}
HEADERS = {"User-Agent":"Mozilla/5.0","Accept":"application/json","Referer":"https://www.yad2.co.il/realestate/rent"}

REPO_DIR = Path("/tmp/cron_repo")
NOW_ISO = datetime.now(timezone.utc).isoformat(timespec="seconds")

def run(cmd, check=True):
    return subprocess.run(cmd, check=check, capture_output=True, text=True)

if REPO_DIR.exists(): subprocess.run(["rm","-rf",str(REPO_DIR)])
run(["git","clone","--depth","1",REPO_URL,str(REPO_DIR)])
run(["git","-C",str(REPO_DIR),"config","user.email",GMAIL_TO])
run(["git","-C",str(REPO_DIR),"config","user.name","Apartment Monitor Bot"])

state    = json.loads((REPO_DIR/"data_state.json").read_text(encoding="utf-8"))
history  = json.loads((REPO_DIR/"data_history.json").read_text(encoding="utf-8"))
template = (REPO_DIR/"data_template.html").read_text(encoding="utf-8")
print(f"Loaded: state {len(state.get('snapshot',{}))} prev, history {len(history.get('events',[]))}")

# Fetch
url = "https://gw.yad2.co.il/realestate-feed/rent/feed?" + urllib.parse.urlencode(PARAMS)
data = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=20).read()).get("data",{})
listings = []; seen = set()
for cat in ["private","agency","platinum","booster","yad1"]:
    for it in (data.get(cat) or []):
        if not isinstance(it,dict): continue
        tok = it.get("token")
        if not tok or tok in seen: continue
        seen.add(tok); it["_cat"]=cat; listings.append(it)
print(f"Fetched {len(listings)}")

def normalize(m):
    addr=m.get("address",{}) or {}; house=addr.get("house",{}) or {}; coords=addr.get("coords",{}) or {}
    det=m.get("additionalDetails",{}) or {}; meta=m.get("metaData",{}) or {}; cust=m.get("customer",{}) or {}
    tags=[t.get("name","") for t in (m.get("tags") or [])]; tok=m.get("token","")
    return {"token":tok,"url":f"https://www.yad2.co.il/realestate/item/{tok}" if tok else "",
        "street":(addr.get("street") or {}).get("text") or "","house_number":house.get("number"),
        "floor":house.get("floor"),"neighborhood":(addr.get("neighborhood") or {}).get("text") or "",
        "city":(addr.get("city") or {}).get("text") or "",
        "lat":coords.get("lat"),"lon":coords.get("lon"),
        "rooms":det.get("roomsCount"),"sqm":det.get("squareMeter"),
        "price":m.get("price"),"price_before":m.get("priceBeforeTag"),
        "ad_type":m.get("adType"),"agency":cust.get("agencyName"),
        "tags":tags,"image":meta.get("coverImage"),"source_category":m.get("_cat"),
        "discovered_at":NOW_ISO,"has_mamad":True,
        "has_parking":any(("חניה" in t) or ("חנייה" in t) for t in tags),
        "has_balcony":any("מרפסת" in t for t in tags)}

def passes(it):
    s,p,r = it.get("sqm"),it.get("price"),it.get("rooms")
    return bool(s and p and r and s>=85 and 3<=r<=4 and 8000<=p<=12000)

def score(it):
    s=10
    if it["has_parking"]: s+=3
    if it["has_balcony"]: s+=2
    if any("מעלית" in t for t in it["tags"]): s+=1
    if (it.get("sqm") or 0) >= 100: s+=1
    return s

items = [normalize(m) for m in listings if m.get("token")]
matching = [i for i in items if passes(i)]
for i in matching: i["score"]=score(i)
matching.sort(key=lambda x:(-x["score"], x.get("price",99999)))

prev_snap = state.get("snapshot",{}) or {}
cur_snap = {i["token"]:i for i in matching}
events_run = []
if len(listings) < 2 and prev_snap:
    print("Suspicious low listings, skipping diff")
else:
    for tok in set(prev_snap)-set(cur_snap):
        events_run.append({"ts":NOW_ISO,"type":"removed","item":prev_snap[tok]})
    for tok in set(cur_snap)-set(prev_snap):
        events_run.append({"ts":NOW_ISO,"type":"added","item":cur_snap[tok]})
    for tok in set(prev_snap)&set(cur_snap):
        op,np = prev_snap[tok].get("price"), cur_snap[tok].get("price")
        if op and np and op != np:
            events_run.append({"ts":NOW_ISO,"type":"price_dropped" if np<op else "price_raised",
                               "item":cur_snap[tok],"old_price":op,"new_price":np})
print(f"Events: {len(events_run)}")

state = {"snapshot":cur_snap,"last_run":NOW_ISO,"version":3}
history["events"] = (events_run + (history.get("events") or []))[:500]
(REPO_DIR/"data_state.json").write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding="utf-8")
(REPO_DIR/"data_history.json").write_text(json.dumps(history,ensure_ascii=False,indent=2),encoding="utf-8")

def slim(it):
    return {k:it.get(k) for k in ["token","url","street","house_number","floor","neighborhood","city",
        "rooms","sqm","price","price_before","ad_type","agency","tags","image","score",
        "has_mamad","has_parking","has_balcony","discovered_at","lat","lon"]}

new_tokens = [e["item"]["token"] for e in events_run if e["type"]=="added" and e["item"].get("token")]
payload = {"updated":NOW_ISO,"items":[slim(i) for i in matching]}
hist_payload = [{"ts":e["ts"],"type":e["type"],"item":slim(e["item"]),
                 "old_price":e.get("old_price"),"new_price":e.get("new_price")} for e in history["events"]]

html = template.replace("__DATA__", json.dumps(payload,ensure_ascii=False,separators=(",",":")))
html = html.replace("__NEW__", json.dumps(new_tokens,ensure_ascii=False))
html = html.replace("__HISTORY__", json.dumps(hist_payload,ensure_ascii=False,separators=(",",":")))
(REPO_DIR/"index.html").write_text(html,encoding="utf-8")

run(["git","-C",str(REPO_DIR),"add","-A"])
diff = run(["git","-C",str(REPO_DIR),"diff","--cached","--stat"], check=False).stdout.strip()
if diff:
    msg = f"Cron {NOW_ISO} - {len(matching)} listings, {len(events_run)} events"
    run(["git","-C",str(REPO_DIR),"commit","-m",msg])
    run(["git","-C",str(REPO_DIR),"push","origin","main"])
    print(f"Pushed: {msg}")
else:
    print("No changes")

if events_run:
    by_type = {}
    for e in events_run: by_type.setdefault(e["type"],[]).append(e)
    counts = {t:len(by_type[t]) for t in by_type}
    bits = []
    for et,emj,lbl in [("added","🆕","חדש"),("price_dropped","⬇️","ירידה"),
                       ("price_raised","⬆️","עלייה"),("removed","✕","ירדו")]:
        if counts.get(et): bits.append(f"{emj} {counts[et]} {lbl}")
    subject = " · ".join(bits) + " · נחלת יצחק"

    def fmt(n):
        try: return f"{int(n):,}"
        except: return str(n) if n is not None else "—"
    def addr(it): return " ".join(str(x) for x in [it.get("street"),it.get("house_number")] if x) or "כתובת חסויה"

    lines = [f"נמצאו {len(events_run)} עדכונים בנחלת יצחק:",""]
    VERBS = {"added":"🆕 דירה חדשה","price_dropped":"⬇️ ירידת מחיר","price_raised":"⬆️ עליית מחיר","removed":"✕ מודעה ירדה"}
    for et in ["added","price_dropped","removed","price_raised"]:
        if not by_type.get(et): continue
        lines.append(f"{VERBS[et]} ({len(by_type[et])}):")
        for ev in by_type[et]:
            it = ev["item"]
            if et in ("price_dropped","price_raised"):
                lines.append(f"  • {addr(it)} ({it['rooms']} חד׳, {it['sqm']} מ״ר): {fmt(ev['old_price'])}₪ → {fmt(ev['new_price'])}₪")
            else:
                lines.append(f"  • {addr(it)} ({it['rooms']} חד׳, {it['sqm']} מ״ר, {fmt(it.get('price'))}₪)")
            lines.append(f"    {it.get('url','')}")
        lines.append("")
    lines.append(f"דאשבורד: {PAGE_URL}")

    msg = EmailMessage()
    msg["Subject"]=subject; msg["From"]=GMAIL_TO; msg["To"]=GMAIL_TO
    msg.set_content("\n".join(lines))
    with smtplib.SMTP_SSL("smtp.gmail.com",465,context=ssl.create_default_context(),timeout=20) as s:
        s.login(GMAIL_TO,GMAIL_PWD); s.send_message(msg)
    print(f"Email sent: {counts}")
else:
    print("No events to email")
print("DONE")
