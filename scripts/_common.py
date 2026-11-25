"""
Shared paths and a polite HTTP helper for the PBS scripts.

Paths are relative to the repository folder (the parent of scripts/), so no environment variable is needed.
Set PBS_DATA_DIR to put the data folder somewhere else.

  data/raw/       files as downloaded or extracted (Xatu days, Flashbots records, node block data, relayscan)
  data/interim/   tables built from raw (Xatu relay table, relay table with Flashbots added)
  data/processed/ the dataset (df_blocks_pbs.parquet)

Scripts print their progress to the screen. They never write log files into the data folder; redirect the
output to a file outside this repository if you want to keep it.
"""
import os
import time
from datetime import date
from pathlib import Path

import requests

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("PBS_DATA_DIR", REPO_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

MERGE_DATE = date(2022, 9, 15)

USER_AGENT = "pbs-block-data/1.0 (research; polite client)"
DEFAULT_DELAY = 1.0  # seconds between two requests to the same public host


class HostBlocked(RuntimeError):
    """The host answered 403 or 429: stop instead of retrying (a block can last hours and covers the whole domain)."""


_last_request = {}


def pivot_compat(df, *, on: str, index: str, values: str, aggregate_function: str):
    """DataFrame.pivot across polars versions: 0.20 calls the argument `columns`, 1.x calls it `on`."""
    try:
        return df.pivot(on=on, index=index, values=values, aggregate_function=aggregate_function)
    except TypeError:
        return df.pivot(columns=on, index=index, values=values, aggregate_function=aggregate_function)


def load_script(filename: str):
    """Import a script whose file name starts with a digit (so it cannot be imported by name)."""
    import importlib.util
    path = Path(__file__).resolve().parent / filename
    spec = importlib.util.spec_from_file_location(path.stem.replace("-", "_"), path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def polite_get(url: str, *, delay: float = DEFAULT_DELAY, timeout: int = 60, params: dict | None = None,
               retries: int = 4, method: str = "GET") -> requests.Response:
    """
    GET (or HEAD) with a minimum gap of `delay` seconds between two requests to the same host, sequential by design.
    Retries timeouts and 5xx answers with a growing wait. Raises HostBlocked on 403 or 429. Returns the response
    for every other status (the caller decides what a 404 means).
    """
    host = url.split("/")[2]
    last_error = None
    for attempt in range(retries + 1):
        wait = delay - (time.time() - _last_request.get(host, 0.0))
        if wait > 0:
            time.sleep(wait)
        try:
            r = requests.request(method, url, timeout=timeout, params=params, headers={"User-Agent": USER_AGENT})
        except requests.RequestException as e:
            last_error = e
            _last_request[host] = time.time()
            time.sleep(min(2 ** (attempt + 1), 60))
            continue
        _last_request[host] = time.time()
        if r.status_code in (403, 429):
            raise HostBlocked(f"{host} answered HTTP {r.status_code} for {url}; stopping. Wait before trying again.")
        if r.status_code >= 500:
            last_error = RuntimeError(f"HTTP {r.status_code}")
            time.sleep(min(2 ** (attempt + 1), 60))
            continue
        return r
    raise RuntimeError(f"{url}: failed after {retries + 1} attempts ({last_error})")
