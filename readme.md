# SentinelOne Metrics Collector

Copy `example_queries.json` to `queries.json`.

Configure queries in `queries.json` as needed. This is loaded at startup and refreshed before every run.

Copy `example.env` to `.env`. Set the values as needed.
Note: For `SENTINELONE_URL` it must only be the subdomain for your console URL. So if it is `https://your-console.sentinelone.net/` then you only use `your-console` as the value.

Usual setup routine for python scripts.
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python collect_metrics.py
```