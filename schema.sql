-- SFC Licensee Database Schema (SQLite)
--
-- Mirrors data from the Hong Kong Securities and Futures Commission (SFC)
-- public register at https://apps.sfc.hk/publicregWeb. Created automatically
-- by update_sfc.py on first run.

-- Activity types (regulated activities under SFO)
CREATE TABLE IF NOT EXISTS activity (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL
);

INSERT OR IGNORE INTO activity (id, name) VALUES
    (1, 'Dealing in securities'),
    (2, 'Dealing in futures contracts'),
    (3, 'Leveraged foreign exchange trading'),
    (4, 'Advising on securities'),
    (5, 'Advising on futures contracts'),
    (6, 'Advising on corporate finance'),
    (7, 'Providing automated trading services'),
    (8, 'Securities margin financing'),
    (9, 'Asset management'),
    (10, 'Providing credit rating services'),
    (11, 'Dealing in OTC derivative products'),
    (12, 'Providing clearing agency services'),
    (13, 'Providing depositary services for relevant CIS');

-- Licensed organisations (firms)
CREATE TABLE IF NOT EXISTS organisations (
    person_id   INTEGER PRIMARY KEY,
    sfc_id      TEXT UNIQUE,            -- SFC CE reference (e.g. "AAA001")
    name1       TEXT NOT NULL,           -- English name
    c_name      TEXT,                    -- Chinese name
    org_type    INTEGER,                 -- 19=Limited, 10=LLC, 9=LLP, etc.
    sfc_ri      INTEGER DEFAULT 0,      -- 1 if Registered Institution, 0 if Licensed Corp
    sfc_upd     TEXT,                    -- last update timestamp (ISO 8601)
    dis_date    TEXT,                    -- dissolution date
    domicile    INTEGER DEFAULT 1        -- 1=HK
);

CREATE INDEX IF NOT EXISTS idx_org_sfc_id ON organisations(sfc_id);
CREATE INDEX IF NOT EXISTS idx_org_name ON organisations(name1);

-- Individual licensees
CREATE TABLE IF NOT EXISTS people (
    person_id   INTEGER PRIMARY KEY,
    sfc_id      TEXT UNIQUE,            -- SFC CE reference (e.g. "BRQ870")
    name1       TEXT NOT NULL,           -- Surname
    name2       TEXT,                    -- Forenames
    c_name      TEXT,                    -- Chinese name
    sex         TEXT,                    -- M/F
    sfc_upd     TEXT                     -- last update timestamp (ISO 8601)
);

CREATE INDEX IF NOT EXISTS idx_ppl_sfc_id ON people(sfc_id);
CREATE INDEX IF NOT EXISTS idx_ppl_name ON people(name1, name2);

-- Individual licence records (one row per person × firm × activity × role × period)
CREATE TABLE IF NOT EXISTS licrec (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    staff_id    INTEGER NOT NULL REFERENCES people(person_id),
    org_id      INTEGER NOT NULL REFERENCES organisations(person_id),
    role        INTEGER NOT NULL,        -- 1=RO (Responsible Officer), 0=Rep (Representative)
    act_type    INTEGER NOT NULL REFERENCES activity(id),
    start_date  TEXT,                    -- NULL means predates system (before 2003-04-01)
    end_date    TEXT                     -- NULL means current/ongoing
);

CREATE INDEX IF NOT EXISTS idx_licrec_staff ON licrec(staff_id);
CREATE INDEX IF NOT EXISTS idx_licrec_org ON licrec(org_id);
CREATE INDEX IF NOT EXISTS idx_licrec_dates ON licrec(start_date, end_date);

-- Organisation licence records
CREATE TABLE IF NOT EXISTS olicrec (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    org_id      INTEGER NOT NULL REFERENCES organisations(person_id),
    ri          INTEGER NOT NULL,        -- 1 if registered institution licence, 0 if corp
    act_type    INTEGER NOT NULL REFERENCES activity(id),
    start_date  TEXT,                    -- NULL means predates system
    end_date    TEXT                     -- NULL means current/ongoing
);

CREATE INDEX IF NOT EXISTS idx_olicrec_org ON olicrec(org_id);

-- Computed role appointments (RO trumps Rep for overlapping periods)
CREATE TABLE IF NOT EXISTS directorships (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    company     INTEGER NOT NULL REFERENCES organisations(person_id),
    director    INTEGER NOT NULL REFERENCES people(person_id),
    position_id INTEGER NOT NULL,        -- 394=Rep, 395=RO
    appt_date   TEXT,                    -- appointment date
    res_date    TEXT                     -- resignation/cessation date
);

CREATE INDEX IF NOT EXISTS idx_dir_company ON directorships(company);
CREATE INDEX IF NOT EXISTS idx_dir_director ON directorships(director);

-- Monthly summary totals by activity type
CREATE TABLE IF NOT EXISTS licrecsum (
    act_type    INTEGER NOT NULL REFERENCES activity(id),
    d           TEXT NOT NULL,           -- month-end date (YYYY-MM-DD)
    total       INTEGER NOT NULL,
    ro          INTEGER NOT NULL,
    PRIMARY KEY (act_type, d)
);

-- Organisation address data
CREATE TABLE IF NOT EXISTS orgdata (
    person_id   INTEGER PRIMARY KEY REFERENCES organisations(person_id),
    addr1       TEXT,
    addr2       TEXT,
    addr3       TEXT,
    district    TEXT,
    territory   INTEGER DEFAULT 1        -- 1=HK
);

-- Old SFC IDs (tracks amalgamations)
CREATE TABLE IF NOT EXISTS old_sfc_ids (
    sfc_id      TEXT PRIMARY KEY,
    until_date  TEXT,                    -- amalgamation date
    org_id      INTEGER NOT NULL REFERENCES organisations(person_id)
);

-- Temporary working table for computing directorship periods
CREATE TABLE IF NOT EXISTS tempsfc (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    org_id      INTEGER NOT NULL,
    role        INTEGER NOT NULL,
    started     TEXT,
    ended       TEXT
);

-- Web addresses for organisations
CREATE TABLE IF NOT EXISTS web (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id   INTEGER NOT NULL REFERENCES organisations(person_id),
    url         TEXT NOT NULL,
    source      INTEGER DEFAULT 2        -- 2=SFC
);

CREATE INDEX IF NOT EXISTS idx_web_person ON web(person_id);

-- Update log
CREATE TABLE IF NOT EXISTS update_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event       TEXT NOT NULL,
    timestamp   TEXT NOT NULL DEFAULT (datetime('now'))
);
