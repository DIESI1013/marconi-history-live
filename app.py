import hmac, json, os, secrets, sqlite3, threading
from datetime import datetime,timedelta
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs,urlsplit
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parent
DB=Path(os.environ.get("DATA_PATH","/tmp/marconi_history_live.sqlite3"))
SECRET=os.environ.get("INGEST_TOKEN","")
LOCK=threading.RLock()

def connect():
    c=sqlite3.connect(str(DB),timeout=20)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA busy_timeout=20000")
    return c

def init_db():
    DB.parent.mkdir(parents=True,exist_ok=True)
    with LOCK,connect() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        c.execute("CREATE TABLE IF NOT EXISTS stations(url TEXT PRIMARY KEY,name TEXT NOT NULL)")
        c.execute("""CREATE TABLE IF NOT EXISTS plays(
            source_id INTEGER PRIMARY KEY, station TEXT,url TEXT,raw TEXT,
            artist TEXT,title TEXT,is_song INTEGER,seen_at TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_plays_station ON plays(url,source_id DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_plays_time ON plays(seen_at DESC)")
        if not c.execute("SELECT 1 FROM meta WHERE key='instance_id'").fetchone():
            c.execute("INSERT INTO meta(key,value) VALUES(?,?)",("instance_id",secrets.token_hex(14)))
        c.commit()

def payload(obj):
    return json.dumps(obj,ensure_ascii=False).encode("utf-8")

class Handler(BaseHTTPRequestHandler):
    server_version="MarconiHistory/1.0"
    def log_message(self,*args): pass
    def send_data(self,code,obj,typ="application/json; charset=utf-8"):
        data=payload(obj) if not isinstance(obj,bytes) else obj
        self.send_response(code)
        self.send_header("Content-Type",typ)
        self.send_header("Content-Length",str(len(data)))
        self.send_header("Cache-Control","no-store")
        self.send_header("X-Content-Type-Options","nosniff")
        self.send_header("X-Frame-Options","DENY")
        self.end_headers()
        self.wfile.write(data)
    def do_GET(self):
        p=urlsplit(self.path); q=parse_qs(p.query)
        try:
            if p.path=="/":
                return self.send_data(200,(ROOT/"dashboard.html").read_bytes(),"text/html; charset=utf-8")
            if p.path in ("/healthz","/api/status"):
                with connect() as c:
                    instance=c.execute("SELECT value FROM meta WHERE key='instance_id'").fetchone()[0]
                    count=c.execute("SELECT COUNT(*) FROM plays").fetchone()[0]
                    ns=c.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
                    last=c.execute("SELECT MAX(seen_at) FROM plays").fetchone()[0]
                return self.send_data(200,{"status":"ok","instance_id":instance,"records":count,"stations":ns,"last_seen":last,"storage":"temporary_render_copy"})
            if p.path=="/api/stations":
                cutoff=(datetime.now(ZoneInfo("Europe/Athens"))-timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
                sql="""SELECT s.url,s.name,
                  (SELECT title FROM plays p WHERE p.url=s.url AND p.is_song=1 ORDER BY source_id DESC LIMIT 1) title,
                  (SELECT artist FROM plays p WHERE p.url=s.url AND p.is_song=1 ORDER BY source_id DESC LIMIT 1) artist,
                  (SELECT seen_at FROM plays p WHERE p.url=s.url ORDER BY source_id DESC LIMIT 1) last_seen,
                  (SELECT COUNT(*) FROM plays p WHERE p.url=s.url AND p.is_song=1 AND p.seen_at>=?) last24h,
                  (SELECT COUNT(*) FROM plays p WHERE p.url=s.url AND p.is_song=1) total
                  FROM stations s ORDER BY s.name COLLATE NOCASE"""
                with connect() as c: rows=[dict(r) for r in c.execute(sql,(cutoff,))]
                return self.send_data(200,{"stations":rows,"total":len(rows)})
            if p.path=="/api/plays":
                url=q.get("url",[""])[0]; limit=max(1,min(300,int(q.get("limit",["100"])[0])))
                day=q.get("day",[""])[0]
                if not url or len(url)>2048:return self.send_data(400,{"error":"url required"})
                where="WHERE url=? AND is_song=1"; params=[url]
                if day:
                    if len(day)!=10 or day[4]!="-" or day[7]!="-":return self.send_data(400,{"error":"invalid day"})
                    where+=" AND substr(seen_at,1,10)=?";params.append(day)
                params.append(limit)
                with connect() as c:
                    rows=[dict(r) for r in c.execute("SELECT source_id,station,artist,title,seen_at,raw FROM plays "+where+" ORDER BY source_id DESC LIMIT ?",params)]
                return self.send_data(200,{"plays":rows,"total":len(rows)})
            return self.send_data(404,{"error":"not found"})
        except (ValueError,TypeError):
            return self.send_data(400,{"error":"invalid query"})
        except Exception as e:
            print("GET ERROR",type(e).__name__,str(e),flush=True)
            return self.send_data(500,{"error":"internal error"})
    def do_POST(self):
        if self.path!="/api/push":return self.send_data(404,{"error":"not found"})
        if not SECRET:return self.send_data(503,{"error":"not configured"})
        if not hmac.compare_digest(self.headers.get("Authorization",""),"Bearer "+SECRET):
            return self.send_data(401,{"error":"unauthorized"})
        try:
            n=int(self.headers.get("Content-Length","0"))
            if n<=0 or n>1500000:return self.send_data(413,{"error":"batch too large"})
            body=json.loads(self.rfile.read(n))
            records=body.get("records",[]);stations=body.get("stations",[])
            if not isinstance(records,list) or len(records)>1000 or not isinstance(stations,list) or len(stations)>600:
                return self.send_data(400,{"error":"invalid batch"})
            cleaned_stations=[(str(s["url"])[:2048],str(s["name"])[:200]) for s in stations if s.get("url") and s.get("name")]
            cleaned=[]
            for r in records:
                rid=int(r["id"])
                if rid<=0:continue
                cleaned.append((rid,str(r.get("station") or "")[:200],str(r.get("url") or "")[:2048],
                    str(r.get("raw") or "")[:512],str(r.get("artist") or "")[:200],
                    str(r.get("title") or "")[:300],int(bool(r.get("is_song"))),
                    str(r.get("seen_at") or "")[:40]))
            with LOCK,connect() as c:
                if cleaned_stations:
                    c.executemany("INSERT INTO stations(url,name) VALUES(?,?) ON CONFLICT(url) DO UPDATE SET name=excluded.name",cleaned_stations)
                if cleaned:
                    c.executemany("""INSERT OR IGNORE INTO plays(source_id,station,url,raw,artist,title,is_song,seen_at)
                                    VALUES(?,?,?,?,?,?,?,?)""",cleaned)
                c.commit()
            return self.send_data(200,{"ok":True,"received":len(cleaned),"stations":len(cleaned_stations)})
        except Exception as e:
            print("POST ERROR",type(e).__name__,str(e),flush=True)
            return self.send_data(400,{"error":"invalid payload"})

if __name__=="__main__":
    init_db()
    port=int(os.environ.get("PORT","10000"))
    print("Marconi History Dashboard listening on",port,flush=True)
    ThreadingHTTPServer(("0.0.0.0",port),Handler).serve_forever()
