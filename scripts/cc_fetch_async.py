#!/usr/bin/env python3
"""PATH A Stage 2 v2: aiohttp WARC fetch engine (async, high-concurrency, resumeable).

Resume-compatible with the classic cc_fetch.py: reads cc_index/*.jsonl,
dedupes by (url,timestamp), fetches exact WARC byte-ranges from
data.commoncrawl.org, caches gzipped bodies in cc_bodies/<key>.gz keyed by
content hash, and appends to cc_bodies/queue.jsonl.

Concurrency is bounded and burst-paced with a soft token bucket so we stay
under CloudFront throttle while keeping ~5-10x the old thread-pool throughput.

Usage:
  python cc_fetch_async.py            # full run, resumeable
  python cc_fetch_async.py --bench N  # throughput test at concurrency N (uses 400 recs)
  python cc_fetch_async.py --limit N  # fetch at most N records then exit
"""
import asyncio, gzip, hashlib, json, os, sys, time

sys.path.insert(0, os.path.dirname(os.__file__))  # stdlib first (local inspect.py shadow)
import inspect

import aiohttp
import aiohttp.client as _c

BASE = os.path.dirname(os.path.abspath(__file__))
IDX_DIR = os.path.join(BASE, "cc_index")
BODY_DIR = os.path.join(BASE, "cc_bodies")
os.makedirs(BODY_DIR, exist_ok=True)
UA = "Mozilla/5.0 (X11; Linux x86_64) research-archive-miner/2.1"

CONCURRENCY = int(os.environ.get("CC_ASYNC_CONCURRENCY", "40"))
MAX_CONN_PER_HOST = int(os.environ.get("CC_MAX_PER_HOST", "40"))
# soft cap: max requests/sec to the CDN edge (bursty bucket)
RPS_CAP = float(os.environ.get("CC_RPS_CAP", "120"))


def classify(rec):
    u = rec.get("url", "")
    mime = (rec.get("mime-detected") or rec.get("mime") or "").lower()
    path = u.split("?")[0].split("#")[0].lower()
    base = os.path.basename(path)
    if "api_web/v1/" in path:
        return "api" if ("/item" in path or "/listing" in path) else "api_other"
    if base in ("robots.txt",) or "sitemap" in base or base.endswith(".xml"):
        return "junk"
    if mime.startswith("text/html"):
        seg = os.path.splitext(base)[0]
        tail = seg.rsplit("-", 1)[-1] if "-" in seg else ""
        if len(tail) >= 18 and len(tail) <= 32:
            return "item_html"
        return "cat_html"
    return "junk"


def iter_records(max_recs):
    seen = set()
    n = 0
    for fn in sorted(os.listdir(IDX_DIR)):
        if not fn.endswith(".jsonl"):
            continue
        with open(os.path.join(IDX_DIR, fn), encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    j = json.loads(line)
                except Exception:
                    continue
                if classify(j) == "junk":
                    continue
                dk = (j.get("url"), j.get("timestamp"))
                if dk in seen:
                    continue
                seen.add(dk)
                yield j
                n += 1
                if max_recs and n >= max_recs:
                    return


def key_of(rec):
    return hashlib.sha1(
        f"{rec['filename']}:{rec['offset']}:{rec['length']}".encode()
    ).hexdigest()


def load_done():
    done = set()
    log = os.path.join(BODY_DIR, "queue.jsonl")
    if os.path.exists(log):
        with open(log, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(rec, dict) and rec.get("key"):
                        done.add(rec["key"])
    return done


async def fetch_one(session, rec, bucket_acquire, pause, lock):
    k = key_of(rec)
    out = os.path.join(BODY_DIR, k + ".gz")
    if os.path.exists(out):
        return k, False  # already on disk; not a new fetch
    fn = rec["filename"]
    off = int(rec["offset"])
    ln = int(rec["length"])
    url = "https://data.commoncrawl.org/" + fn
    headers = {"Range": f"bytes={off}-{off + ln - 1}"}

    # soft rate cap: wait for a token if the bucket is empty (burstable RPS)
    if bucket_acquire:
        await bucket_acquire()
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=120)) as r:
            if r.status not in (200, 206):
                # backoff on throttle
                if r.status in (429, 403, 503):
                    async with lock:
                        pause[0] = max(time.time() + 30, pause[0])
                return k, False
            data = await r.read()
    except (aiohttp.ClientError, OSError):
        return k, False

    if fn.endswith(".gz") or data[:2] == b"\x1f\x8b":
        try:
            data = gzip.decompress(data)
        except (OSError, EOFError, ValueError):
            return k, False
    m = data.find(b"\r\n\r\n")
    if m >= 0:
        data = data[m + 4:]
    if not data:
        return k, False
    with open(out, "wb") as wf:
        wf.write(gzip.compress(data, 6))
    return k, True


async def worker(session, in_q, out_q, done_set, bucket, pause, lock):
    while True:
        try:
            rec = in_q.get_nowait()
        except asyncio.QueueEmpty:
            return
        while pause[0] and time.time() < pause[0]:
            await asyncio.sleep(0.5)
        res = await fetch_one(session, rec, bucket, pause, lock)
        await out_q.put((res[0], res[1], rec))


# soft rate cap is applied inside fetch_one via bucket; pause handled globally
#   -> intentionally no per-task semaphore beyond the connection pool


async def runner(max_recs, bundle_done):
    done_set = load_done()
    recs = list(iter_records(max_recs))
    # build work queue of not-yet-done records
    work = [r for r in recs if key_of(r) not in done_set]
    total_work = len(work)
    done = ok = skip = fail = 0
    # done_set covers previous sessions; count them as already "done" for skip display
    skip = len(recs) - total_work
    t0 = time.time()

# token bucket: burst up to RPS_CAP, refill at RPS_CAP tokens/sec
    class _Bucket:
        def __init__(self, rate):
            self.rate = rate
            self.tokens = rate
            self.updated = time.monotonic()
            self.mtx = asyncio.Lock()

        async def acquire(self):
            while True:
                async with self.mtx:
                    now = time.monotonic()
                    self.tokens = min(self.rate, self.tokens + (now - self.updated) * self.rate)
                    self.updated = now
                    if self.tokens >= 1:
                        self.tokens -= 1
                        return
                await asyncio.sleep(0.05)

    bucket = _Bucket(RPS_CAP) if RPS_CAP else None
    pause = [0.0]
    lock = asyncio.Lock()
    in_q = asyncio.Queue()
    for r in work:
        in_q.put_nowait(r)
    out_q = asyncio.Queue()

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY, limit_per_host=MAX_CONN_PER_HOST,
        force_close=False, enable_cleanup_closed=True,
        ttl_dns_cache=300,
    )
    timeout = aiohttp.ClientTimeout(total=180, connect=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout,
                                     headers={"User-Agent": UA}) as session:
        bget = bucket.acquire if bucket else None
        workers = [asyncio.ensure_future(worker(session, in_q, out_q, done_set, bget, pause, lock))
                   for _ in range(CONCURRENCY)]
        completed = 0
        while completed < total_work:
            k, was_ok, rec = await out_q.get()
            completed += 1
            if was_ok:
                ok += 1
            else:
                fail += 1
            # only journal successful fetches so throttled/failed keys are retried
            if was_ok:
                with open(os.path.join(BODY_DIR, "queue.jsonl"), "a", encoding="utf-8") as lg:
                    lg.write(json.dumps({"key": k, "cls": classify(rec),
                                         "url": rec.get("url"), "ts": rec.get("timestamp")}) + "\n")
            if completed % 200 == 0:
                el = time.time() - t0
                print(f"  ok={ok} skip={skip} fail={fail} rate={completed/max(el,1):.1f}/s {el:.0f}s",
                      flush=True)
        await asyncio.gather(*workers, return_exceptions=True)
    print(f"done: ok={ok} skip={skip} fail={fail} fetched={ok+fail}", flush=True)
    bundle_done["ok"] = ok
    bundle_done["skip"] = skip
    bundle_done["fail"] = fail
    bundle_done["t"] = time.time() - t0


async def main():
    args = sys.argv[1:]
    bench = 0
    limit = 0
    i = 0
    while i < len(args):
        if args[i] == "--bench":
            bench = int(args[i + 1]); i += 2
        elif args[i] == "--limit":
            limit = int(args[i + 1]); i += 2
        else:
            i += 1
    max_recs = bench if bench else (limit if limit else 0)
    bundle_done = {}
    await runner(max_recs, bundle_done)
    if bench:
        print(f"BENCH concurrency={CONCURRENCY}: {bundle_done['ok']}/{bundle_done['skip']} "
              f"ok/skip, fail={bundle_done['fail']}, {bundle_done['ok']/bundle_done['t']:.1f} new-fetch/s")


if __name__ == "__main__":
    asyncio.run(main())