# hk-sfc-licensees

A self-contained Python tool that builds and maintains a local SQLite database
of every corporation and individual licensed by the **Hong Kong Securities and
Futures Commission (SFC)** — both currently active and historical — by
scraping the SFC public register at <https://apps.sfc.hk/publicregWeb>.

No authentication is required. The script talks only to the public register
endpoints that power the SFC's own website.

> **Just want to browse the data?** A hosted version of this database, with a
> web UI for searching firms, individuals, career histories, and industry
> statistics, is available at **<https://thesfcnetwork.com/#dashboard>** —
> use that if you don't want to run anything yourself.

## What you get

After a full run, you have a single SQLite file (`sfc.db` by default). The
full schema lives in [`schema.sql`](schema.sql) and is created automatically
on first run.

### Tables

| Table           | Contents                                                          |
| --------------- | ----------------------------------------------------------------- |
| `organisations` | Licensed firms (active and dissolved), with SFC CE reference      |
| `people`        | Individual licensees (active and ceased)                          |
| `licrec`        | Per-person × firm × activity × role × period licence records      |
| `olicrec`       | Per-firm licence records by activity and period                   |
| `directorships` | Computed RO/Rep appointments (RO trumps Rep on overlapping dates) |
| `licrecsum`     | Monthly summary totals by activity type                           |
| `orgdata`       | Firm registered addresses                                         |
| `web`           | Firm website URLs                                                 |
| `old_sfc_ids`   | Historical SFC IDs from amalgamations                             |
| `activity`      | The 13 SFC regulated activity types                               |

### Schema in brief

**Two entity tables.** `organisations` and `people` are the two anchor
tables. Each has its own internal primary key `person_id` (these are
*separate* ID spaces despite the shared column name) and an `sfc_id` column
holding the SFC's public CE reference (e.g. `AAA001` for firms, `BRQ870` for
individuals).

**Two licence-record tables.** Both use a "one row per period" model:

- `licrec` rows answer *"who held what licence at which firm, in which role,
  over which period?"* — joined to `people` via `staff_id` and to
  `organisations` via `org_id`.
- `olicrec` rows answer the same question at the firm level
  (firm × activity × period), with no person attached.

**Time conventions.** All dates are ISO `YYYY-MM-DD` text. `start_date IS
NULL` means the licence *predates the SFC online register* (before
2003-04-01); `end_date IS NULL` means it is *currently active*.

**Encoding cheat sheet.**

| Column                       | Meaning                                                  |
| ---------------------------- | -------------------------------------------------------- |
| `licrec.role`                | `1` = RO (Responsible Officer), `0` = Rep (Representative) |
| `directorships.position_id`  | `395` = RO, `394` = Rep                                  |
| `act_type` (1–13)            | SFC regulated activity — see the `activity` table        |
| `organisations.sfc_ri`       | `1` = Registered Institution (bank), `0` = Licensed Corp |
| `organisations.org_type`     | `19` = Limited, `10` = LLC, `9` = LLP                    |
| `organisations.dis_date`     | NULL = still in business; date = dissolved/ceased        |

**Amalgamations.** When a firm rebrands or merges and is reissued a new SFC
CE reference, the old reference is preserved in `old_sfc_ids` pointing at
the same `org_id` as before. Lookups by `sfc_id` should always check this
table too.

### Example query: top 10 firms by current Responsible Officer count

```sql
SELECT o.name1                     AS firm,
       COUNT(DISTINCT l.staff_id)  AS ro_count
FROM   licrec       l
JOIN   organisations o ON o.person_id = l.org_id
WHERE  l.role = 1                -- RO
  AND  l.end_date IS NULL        -- still active
  AND  o.dis_date IS NULL        -- firm still in business
GROUP  BY o.person_id
ORDER  BY ro_count DESC
LIMIT  10;
```

## Requirements

- Python 3.8 or newer
- A single dependency: [`requests`](https://pypi.org/project/requests/)

## Install

```bash
git clone https://github.com/<your-user>/hk-sfc-licensees.git
cd hk-sfc-licensees
pip install -r requirements.txt
```

## Bundled snapshot

For convenience, this repository ships a one-time snapshot of the database
as a gzipped SQL dump:

| File          | Size                                                          |
| ------------- | ------------------------------------------------------------- |
| `db.sql.gz`   | ~12 MB compressed (~82 MB SQL text, ~5.7k firms, ~134k people) |

It is **not** kept in sync with the SFC register — treat it as a starting
point and run `python update_sfc.py` afterwards to bring it up to date.

### Restore the snapshot

Unix/macOS:

```bash
gunzip -c db.sql.gz | sqlite3 sfc.db
```

Windows PowerShell (no need for the `sqlite3` CLI):

```powershell
python -c "import sqlite3, gzip; con=sqlite3.connect('sfc.db'); con.executescript(gzip.open('db.sql.gz','rt',encoding='utf-8').read()); con.close()"
```

After restoring you have a populated `sfc.db` and can immediately run
`python update_sfc.py` to refresh it.

## Usage

### First run from scratch (full build)

```bash
python update_sfc.py --full
```

This creates `sfc.db` in the current directory and pulls the entire SFC
public register. It walks every regulated activity type (1–13) and every
starting letter for both corporations and individuals. The full build is
network-intensive — expect a large number of HTTP requests. If you would
rather start from the bundled snapshot, see the section above.

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
