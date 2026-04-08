# hk-sfc-licensees

A self-contained Python tool that builds and maintains a local SQLite database
of every corporation and individual licensed by the **Hong Kong Securities and
Futures Commission (SFC)** — both currently active and historical — by
scraping the SFC public register at <https://apps.sfc.hk/publicregWeb>.

No authentication is required. The script talks only to the public register
endpoints that power the SFC's own website.

## What you get

After a full run, you have a single SQLite file (`sfc.db` by default)
containing:

| Table           | Contents                                                          |
| --------------- | ----------------------------------------------------------------- |
| `organisations` | Licensed firms (active and dissolved), with SFC CE reference      |
| `people`        | Individual licensees (active and ceased)                          |
| `licrec`        | Per-person × firm × activity × role × period licence records      |
| `olicrec`       | Per-firm licence records                                          |
| `directorships` | Computed RO/Rep appointments (RO trumps Rep on overlapping dates) |
| `licrecsum`     | Monthly summary totals by activity type                           |
| `orgdata`       | Firm addresses                                                    |
| `web`           | Firm website URLs                                                 |
| `old_sfc_ids`   | Historical SFC IDs from amalgamations                             |
| `activity`      | The 13 SFC regulated activity types                               |

The schema lives in [`schema.sql`](schema.sql) and is created automatically on
first run.

## Requirements

- Python 3.8 or newer
- A single dependency: [`requests`](https://pypi.org/project/requests/)

## Install

```bash
git clone https://github.com/<your-user>/hk-sfc-licensees.git
cd hk-sfc-licensees
pip install -r requirements.txt
```

## Usage

### First run (full build)

```bash
python update_sfc.py --full
```

This creates `sfc.db` in the current directory and pulls the entire SFC
public register. It walks every regulated activity type (1–13) and every
starting letter for both corporations and individuals. The full build is
network-intensive — expect a large number of HTTP requests.

### Incremental updates

```bash
python update_sfc.py
```

Subsequent runs skip any entity that was successfully updated within the last
12 hours. Safe to re-run; the script picks up where it left off after an
interruption.

### Choosing where the database lives

```bash
python update_sfc.py --db /path/to/my-sfc.db
# or:
SFC_DB_PATH=/path/to/my-sfc.db python update_sfc.py
```

### Targeted operations

| Flag        | What it does                                                            |
| ----------- | ----------------------------------------------------------------------- |
| `--full`    | Force re-fetch of every entity, ignoring the 12-hour skip window.       |
| `--missing` | Only fetch histories for people in the DB whose `sfc_upd` is NULL.      |
| `--step5`   | Only run Step 5 — discover all individuals (incl. ceased).              |
| `--step6`   | Only run Step 6 — refresh org addresses and websites.                   |
| `--step7`   | Only run Step 7 — recompute the monthly licence totals.                 |

## How it works

The full build runs seven sequential steps:

1. **Discover corporations** — page through the public-register search API
   for every (activity, starting letter) combination, capturing both active
   and ceased firms.
2. **Update org staff** — for every known firm, fetch the current ROs
   (Responsible Officers) and Reps (Representatives).
3. **Update individual histories** — for every known person, pull their full
   licence record (which firms, which activities, which periods, which role).
4. **Update org licence histories** — pull each firm's licence record.
5. **Discover individuals** — page through the search API by activity and
   surname letter to catch any individual not surfaced via step 2.
6. **Update addresses** — pull each firm's registered address and website.
7. **Compute monthly totals** — derive `licrecsum` from `licrec`.

All HTTP fetching uses a thread pool (`WORKER_THREADS = 8` by default), with
exponential-backoff retries on transient failures. SQLite writes are
serialised through a single connection and a write lock; WAL mode is enabled.

## Going through a proxy

The script uses `requests`, which automatically honours the standard proxy
environment variables. If you need to route traffic through a proxy:

```bash
export HTTPS_PROXY=http://user:pass@proxy.example.com:8080
export HTTP_PROXY=http://user:pass@proxy.example.com:8080
python update_sfc.py --full
```

## Being polite to the SFC server

By default the script makes requests as fast as the network allows, with no
artificial delay between calls. If you want to be gentler on the upstream API,
edit `REQUEST_DELAY` near the top of `update_sfc.py` to a value greater than
zero (in seconds), or reduce `WORKER_THREADS`.

## Data accuracy and limitations

- The script mirrors whatever the SFC public register currently exposes.
  Errors, omissions, late edits, and amalgamations on the SFC side are
  reflected verbatim — this tool is a faithful scraper, not a corrected
  dataset.
- Licence records with a start date earlier than the SFC's online system
  (`2003-04-01`) are stored with `start_date = NULL`, meaning *predates the
  online register*.
- Names are normalised to title case using a heuristic suited to Hong Kong
  naming conventions; edge cases may not be perfectly capitalised.

## Credits

Inspired by, and modelled after, the long-running SFC scraper that powers
**Webb-site** (<https://webb-site.com>), maintained by David Webb. This
project is an independent client of the public SFC API and is not affiliated
with, endorsed by, or derived from Webb-site.

## License

[MIT](LICENSE) © 2026 Chenning Xu

## Disclaimer

This tool reads only data that the SFC publishes openly through its public
register. Users are responsible for complying with the SFC's terms of use, any
applicable rate limits, and Hong Kong privacy and data-protection law when
storing or republishing the resulting database.
