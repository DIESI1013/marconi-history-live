# MARCONI HISTORY LIVE

An independent ICY-metadata dashboard for radio stations. It does not do acoustic song recognition.

Architecture: MARCONI2 writes `D:\DATA\Desktop\marconi_history.db` continuously. A separate read-only sync program (`marconi_render_sync.py`) sends station names and history rows to Render through a protected endpoint. Render displays the latest songs and station-level histories. Render's SQLite is a disposable cache; the Windows SQLite is the long-term source of truth.

The Render web process is implemented using Python's standard library. No external packages or database servers are required. Deploy on Render with runtime `python`, build command `python -m compileall -q app.py`, and start command `python -u app.py`. Set the secret environment variable `INGEST_TOKEN`. Choose Frankfurt region.

Security: the bearer token is never published in this repository. The Render UI/API reads are public, while writes require the token. Avoid exposing private or licensed data on this public dashboard.

Render Free limitation: web service may sleep with no inbound requests, and all local SQLite data is erased on restart/redeploy. The uploader detects a fresh instance ID and resends up to the last 48 hours from MARCONI2. This only operates when the PC and both scripts are running. No changes are made to the existing main Marconi service.