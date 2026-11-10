"""Separate myTuner playlist store. Does not query Marconi Soundintel."""
import json,os,sqlite3,threading,hmac,secrets,time
from datetime import datetime,timezone,timedelta
from pathlib import Path
from urllib.parse import parse_qs,urlsplit
DB=Path(os.environ.get("MYTUNER_DB","/tmp/mytuner_playlist_history.sqlite3"))
LOCK=threading.RLock()

def db():
    c=sqlite3.connect(str(DB),timeout=20)
    c.execute("PRAGMA busy_timeout=20000")
    c.row_factory=sqlite3.Row
    return c

def init():
    DB.parent.mkdir(parents=True,exist_ok=True)
    with LOCK,db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS mt_meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS mt_stations(
          radio_id INTEGER PRIMARY KEY,name TEXT NOT NULL,page_url TEXT NOT NULL,
          last_sync INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS mt_plays(
          radio_id INTEGER NOT NULL,started INTEGER NOT NULL,
          artist TEXT NOT NULL,title TEXT NOT NULL,raw TEXT NOT NULL,
          artwork TEXT NOT NULL DEFAULT '',
          PRIMARY KEY(radio_id,started,raw));
        CREATE INDEX IF NOT EXISTS mt_plays_station_date ON mt_plays(radio_id,started DESC);
        """)
        if not c.execute("SELECT 1 FROM mt_meta WHERE k='instance'").fetchone():
            c.execute("INSERT INTO mt_meta(k,v) VALUES('instance',?)",(secrets.token_hex(12),))
        c.commit()

def reply(handler,obj,status=200):
    return handler.send_data(status,obj)

def get(handler,p,q):
    if p=="/api/mt/status":
        with db() as c:
            instance=c.execute("SELECT v FROM mt_meta WHERE k='instance'").fetchone()[0]
            total=c.execute("SELECT COUNT(*) FROM mt_plays").fetchone()[0]
            count=c.execute("SELECT COUNT(*) FROM mt_stations").fetchone()[0]
            sync=c.execute("SELECT MAX(last_sync) FROM mt_stations").fetchone()[0]
        return reply(handler,{"status":"ok","source":"myTuner public webpage","instance_id":instance,
             "stations":count,"plays":total,"last_sync":sync,"archive":"MARCONI2 D: standalone mytuner database"})
    if p=="/api/mt/stations":
        sql="""SELECT s.radio_id,s.name,s.page_url,s.last_sync,
        (SELECT p.title FROM mt_plays p WHERE p.radio_id=s.radio_id ORDER BY p.started DESC LIMIT 1) title,
        (SELECT p.artist FROM mt_plays p WHERE p.radio_id=s.radio_id ORDER BY p.started DESC LIMIT 1) artist,
        (SELECT p.started FROM mt_plays p WHERE p.radio_id=s.radio_id ORDER BY p.started DESC LIMIT 1) last_played,
        (SELECT COUNT(*) FROM mt_plays p WHERE p.radio_id=s.radio_id) total
        FROM mt_stations s ORDER BY s.name COLLATE NOCASE"""
        with db() as c: arr=[dict(r) for r in c.execute(sql)]
        return reply(handler,{"stations":arr,"total":len(arr)})
    if p=="/api/mt/plays":
        try: rid=int(q.get("radio_id",["0"])[0]);limit=max(1,min(2000,int(q.get("limit",["250"])[0])))
        except ValueError:return reply(handler,{"error":"invalid parameter"},400)
        day=q.get("day",[""])[0]
        if rid<=0:return reply(handler,{"error":"invalid radio ID"},400)
        where="radio_id=?"; params=[rid]
        if day:
            try:
                d=datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
                start=int(d.timestamp());end=start+86400
            except ValueError:return reply(handler,{"error":"invalid date"},400)
            where+=" AND started>=? AND started<?";params.extend([start,end])
        params.append(limit)
        with db() as c:
            items=[dict(r) for r in c.execute("SELECT radio_id,started,artist,title,raw,artwork FROM mt_plays WHERE "+where+" ORDER BY started DESC LIMIT ?",params)]
        return reply(handler,{"plays":items,"total":len(items)})
    if p=="/api/mt/top":
        try:
            rid=int(q.get("radio_id",["0"])[0]); days=int(q.get("days",["7"])[0])
            if rid<=0 or days not in (1,7,30):raise ValueError()
        except ValueError:return reply(handler,{"error":"invalid parameter"},400)
        cutoff=int(time.time())-days*86400
        with db() as c:
            rows=[dict(r) for r in c.execute("""SELECT artist,title,COUNT(*) plays,
                       MAX(started) last_played
                       FROM mt_plays WHERE radio_id=? AND started>=?
                       GROUP BY LOWER(artist),LOWER(title)
                       ORDER BY plays DESC,last_played DESC LIMIT 100""",(rid,cutoff))]
        return reply(handler,{"days":days,"top":rows,"total":len(rows)})
    return False

def push(handler):
    token=os.environ.get("INGEST_TOKEN","")
    if not token:return reply(handler,{"error":"not configured"},503)
    if not hmac.compare_digest(handler.headers.get("Authorization",""),"Bearer "+token):
        return reply(handler,{"error":"unauthorized"},401)
    try:
        n=int(handler.headers.get("Content-Length","0"))
        if not 0<n<=1200000:return reply(handler,{"error":"payload size"},413)
        data=json.loads(handler.rfile.read(n))
        songs=data.get("plays",[]); stations=data.get("stations",[])
        if not isinstance(songs,list) or len(songs)>500 or not isinstance(stations,list) or len(stations)>100:
            return reply(handler,{"error":"invalid batch"},400)
        sr=[]
        for s in stations:
            rid=int(s["radio_id"]);name=str(s["name"])[:200];url=str(s["page_url"])[:500]
            if not url.startswith("https://mytuner-radio.com/"):continue
            sr.append((rid,name,url,min(int(s.get("last_sync") or 0),int(time.time())+300)))
        pr=[]
        for x in songs:
            rid=int(x["radio_id"]);started=int(x["started"])
            artist=str(x.get("artist") or "")[:200].strip();title=str(x.get("title") or "")[:300].strip()
            raw=str(x.get("raw") or "")[:512].strip()
            artwork=str(x.get("artwork") or "")[:500]
            if rid<1 or not raw or started<1000000000 or started>int(time.time())+600:continue
            pr.append((rid,started,artist,title,raw,artwork))
        with LOCK,db() as c:
            if sr:c.executemany("""INSERT INTO mt_stations(radio_id,name,page_url,last_sync)
            VALUES(?,?,?,?) ON CONFLICT(radio_id) DO UPDATE SET
            name=excluded.name,page_url=excluded.page_url,
            last_sync=MAX(mt_stations.last_sync,excluded.last_sync)""",sr)
            if pr:c.executemany("""INSERT OR IGNORE INTO mt_plays
            (radio_id,started,artist,title,raw,artwork) VALUES(?,?,?,?,?,?)""",pr)
            c.commit()
        return reply(handler,{"ok":True,"stations":len(sr),"received":len(pr)})
    except Exception as e:
        print("MT PUSH",type(e).__name__,str(e)[:200],flush=True)
        return reply(handler,{"error":"bad payload"},400)
