# dbt on Snowflake + PySpark via Snowpark Connect

A worked, end-to-end tutorial repo covering three things:

1. **dbt Core against Snowflake** — sources, `ref()`, materializations, tests, incremental models
2. **dbt Python models** — which on Snowflake are **Snowpark**, not PySpark
3. **Real PySpark against Snowflake** — via Snowpark Connect (SCOS), sitting *downstream* of the dbt DAG

Plus deploying the same project as a **native Snowflake `DBT PROJECT` object** so it runs
inside Snowflake with no local Python at all.

Everything here was executed and verified against a live Snowflake account. Row counts and
test results in this README are actual output.

---

## The headline clarification

> **dbt on Snowflake does not run PySpark.**

dbt Python models on Snowflake execute as **Python stored procedures** using the
`snowflake.snowpark` API. There is no Spark cluster and no `pyspark` import.

The Snowpark DataFrame API is deliberately Spark-shaped, so translation is near-mechanical —
but it is a different library. If you need the literal `pyspark` API surface, you use
**Snowpark Connect for Spark (SCOS)**, which speaks the Spark Connect protocol and compiles
the Spark plan down to Snowflake SQL.

| You want | Use | Where in this repo |
|---|---|---|
| SQL transformations | dbt SQL models | `dbt_test/models/**/*.sql` |
| DataFrame code inside the dbt DAG | dbt Python model (Snowpark) | `dbt_test/models/marts/py_customer_segments.py` |
| Literal `pyspark` code | Snowpark Connect | `dbt_test/spark/pyspark_downstream.py` |

**Recommended architecture:** let dbt own the transformation DAG, and let PySpark read the
tables dbt produced. Do not try to hide PySpark inside dbt.

---

## Repo layout

```
.
├── requirements-dbt.txt          dbt-core + dbt-snowflake pins
├── requirements-spark.txt        snowpark-connect + pyspark 3.5.5 (separate venv!)
├── .env.example                  connection settings template
│
├── dbt_test/                     ← the project you run LOCALLY
│   ├── dbt_project.yml
│   ├── profiles.yml              all values via env_var(), safe to commit
│   ├── models/
│   │   ├── staging/_sources.yml      TPCH declared as a source + source tests
│   │   ├── staging/stg_orders.sql    view: rename / cast / filter
│   │   ├── staging/stg_customer.sql
│   │   ├── marts/fct_customer_orders.sql   table: join + aggregate
│   │   ├── marts/fct_daily_orders.sql      INCREMENTAL table
│   │   ├── marts/py_customer_segments.py   Snowpark Python model
│   │   └── schema.yml                      20 data tests
│   └── spark/
│       └── pyspark_downstream.py     real PySpark reading dbt's output
│
└── dbt_test_snowflake/           ← the same project, prepared for `snow dbt deploy`
    ├── profiles.yml                  auth fields REMOVED (mandatory)
    └── env.yml.needs-cli-3.22        per-environment vars; needs newer CLI
```

Two copies of the project is intentional, and it is the documented Snowflake pattern: the
deployable copy must have all authentication stripped out, so it cannot be the same file you
use locally.

---

## Prerequisites

- Python 3.11
- Snowflake CLI (`snow`) — 3.14+ works; **3.22+** unlocks `env.yml` (see Step 6)
- A Snowflake role that can create a database
- Key-pair auth configured for your user ([Snowflake docs](https://docs.snowflake.com/en/user-guide/key-pair-auth))
- An entry in `~/.snowflake/connections.toml` for your account

### Grant access to the sample data

The models read `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`. The share is usually already mounted, but
non-admin roles often have no privileges on it. As `ACCOUNTADMIN`:

```sql
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake_sample_data TO ROLE <your_role>;
```

> If `SELECT` fails with *"Database 'SNOWFLAKE_SAMPLE_DATA' does not exist or not authorized"*,
> this grant is what you are missing. Note that `CREATE DATABASE ... FROM SHARE` will **not**
> fix it — it errors with *"already exists, but current role has no privileges on it."*

### Create the target database

```sql
CREATE DATABASE IF NOT EXISTS DBT_LEARN;
CREATE SCHEMA   IF NOT EXISTS DBT_LEARN.DEV;
```

dbt creates `DBT_LEARN.DEV_STAGING` and `DBT_LEARN.DEV_MARTS` itself, from the
`+schema:` settings in `dbt_project.yml`.

---

## Step 1 — Install dbt and configure the connection

```bash
git clone https://github.com/sfc-gh-gmahadevan/dbt_scos_test.git
cd dbt_scos_test

python3 -m venv .venv
.venv/bin/pip install -r requirements-dbt.txt

cp .env.example .env
$EDITOR .env                      # fill in your account, user, key path, passphrase
set -a && source .env && set +a   # export everything
```

`.env` is gitignored. `profiles.yml` reads only environment variables, so no credential
ever lands in the repo.

Verify:

```bash
cd dbt_test
DBT_PROFILES_DIR=. ../.venv/bin/dbt debug
```

Expect `All checks passed!`

> **Gotcha — 404 on login.** If you see
> `290404 (08001): 404 Not Found: post <ACCT>.snowflakecomputing.com:443/session/v1/login-request`,
> your `SF_ACCOUNT` is missing the region. Locator-style accounts need
> `AB12345.us-east-1`, not bare `AB12345`.

---

## Step 2 — Build the SQL models

```bash
cd dbt_test
DBT_PROFILES_DIR=. ../.venv/bin/dbt build
```

Verified output:

```
Finished running 1 incremental model, 1 table model, 20 data tests, 2 view models in 17.62s
Done. PASS=24 WARN=0 ERROR=0 SKIP=0 TOTAL=24
```

`dbt build` interleaves models and tests in dependency order, so a failing test halts
anything downstream of it. `dbt run` skips tests; `dbt test` runs only tests.

### The four concepts that are most of dbt

| Concept | File to read | What it buys you |
|---|---|---|
| `source()` | `models/staging/_sources.yml` | Raw tables declared once. Point at a different database by editing one file. |
| `ref()` | every mart model | dbt infers build order from these. You never hand-write dependencies. |
| Materialization | `dbt_project.yml` | The same SQL becomes a view, table, or incremental table by config — no DDL rewrite. |
| Tests | `models/schema.yml` | 20 assertions (`unique`, `not_null`, `relationships`, `accepted_values`) run as part of the build. |

### Useful selectors

```bash
dbt build --select stg_orders          # one model
dbt build --select +fct_daily_orders   # it and everything upstream
dbt build --select fct_daily_orders+   # it and everything downstream
dbt run   --vars '{start_date: "1996-01-01"}'   # override a project var
```

---

## Step 3 — Understand the incremental model

`models/marts/fct_daily_orders.sql` is the pattern worth internalizing:

```sql
{{ config(materialized='incremental', unique_key='order_date',
          incremental_strategy='delete+insert') }}
...
{% if is_incremental() %}
where order_date >= (select coalesce(max(order_date), '1900-01-01') from {{ this }})
{% endif %}
```

- **First run** builds the whole table; `is_incremental()` is false.
- **Later runs** add the `WHERE` clause so only new days get scanned.
- `delete+insert` on `order_date` replaces any day already present, so a partially-loaded
  day is *corrected* rather than duplicated.
- `{{ this }}` is the table this model already built.

> ⚠️ **After changing incremental logic, you must run `--full-refresh`.** A normal
> incremental run only processes new rows, so rows written by the old, broken logic survive
> untouched. This bites everyone once.

```bash
dbt run --select fct_daily_orders --full-refresh
```

---

## Step 4 — The dbt Python model (Snowpark)

```bash
DBT_PROFILES_DIR=. ../.venv/bin/dbt run --select py_customer_segments
```

The contract for a dbt Python model:

- exactly one function, `model(dbt, session)`
- returns a DataFrame (Snowpark or pandas); dbt materializes whatever you return
- use `dbt.ref(...)` / `dbt.source(...)`, never hardcoded table names
- declare dependencies in `dbt.config(packages=[...])` — resolved from Snowflake's
  Anaconda channel, no `pip install` at runtime

Verified result — 99,806 active customers segmented, and revenue is heavily concentrated:

| Segment | Customers | % of revenue |
|---|---|---|
| Champion | 23,722 | 40.8% |
| Loyal | 20,663 | 24.3% |
| Potential | 19,975 | 17.4% |
| At Risk | 20,411 | 12.6% |
| Dormant | 15,035 | 4.9% |

### Snowpark vs PySpark differences that will trip you up

| PySpark | Snowpark | Note |
|---|---|---|
| `Window.orderBy(...)` | `Window.order_by(...)` | snake_case throughout |
| `Window.partitionBy(...)` | `Window.partition_by(...)` | |
| `F.ntile(5)` | `F.ntile(F.lit(5))` | a bare `int` fails |
| `F.sum(x).over(Window.partitionBy())` | `...over(Window.partition_by(F.lit(1)))` | "whole table" needs an explicit literal partition |

`F.when(...).otherwise(...)`, `withColumn`, `filter`, `select`, and `groupBy` are identical.

---

## Step 5 — Run real PySpark via Snowpark Connect

**Use a separate venv.** SCOS pins `pyspark==3.5.5` and that conflicts with dbt's tree.
pyspark 4.x breaks Spark Connect serialization in ways that surface as confusing parse
errors.

```bash
cd dbt_test
python3 -m venv spark/.venv
spark/.venv/bin/pip install -r ../requirements-spark.txt

SNOWFLAKE_CONNECTION_NAME=<your-connection> spark/.venv/bin/python spark/pyspark_downstream.py
```

The script reads the dbt-built `FCT_DAILY_ORDERS`, computes month-over-month revenue growth
with a `lag` window, and writes `SPARK_MONTHLY_GROWTH`. Verified output:

| month | orders | revenue | mom_pct |
|---|---|---|---|
| 1998-07 | 1,199 | 181,794,523 | −93.81 |
| 1998-06 | 19,373 | 2,935,593,509 | 4.50 |
| 1998-05 | 18,590 | 2,809,087,922 | −4.48 |

(1998-07 is a partial month — TPCH data ends in early July 1998.)

### Three non-obvious SCOS requirements

**1. Import from `pyspark.sql.connect`, not `pyspark.sql`.**

```python
from pyspark.sql.connect import functions as F
from pyspark.sql.connect.window import Window
```

Under a Spark Connect session the classic `pyspark.sql.functions` tries to reach a local JVM
and dies with a bare, unhelpful `AssertionError`:

```
File ".../pyspark/sql/functions.py", line 95, in _invoke_function
    assert SparkContext._active_spark_context is not None
AssertionError
```

**2. Let SCOS own the Snowflake session.** Set `SNOWFLAKE_CONNECTION_NAME` and call
`snowpark_connect.start_session()`. Do **not** pre-create a `snowflake.snowpark.Session` and
expect SCOS to adopt it — with current SCOS that fails:

```
SNOWPARK CONNECT ERROR CODE: 5001
'Session' object has no attribute '_spark_session_cache_registry'
```

**3. Ignore the shutdown traceback.** After `main()` returns, the embedded gRPC server logs
`RuntimeError: cannot schedule new futures after shutdown`. Your work is already committed;
this is a teardown race, not a failure.

---

## Step 6 — Deploy as a native Snowflake dbt project

This runs dbt **inside Snowflake**. No local Python, and it schedules with a plain task.

`dbt_test_snowflake/` is the prepared copy. What differs from the local project:

| Requirement | Why |
|---|---|
| No `password` / `authenticator` / `private_key_path` | Auth comes from the session running `EXECUTE DBT PROJECT` |
| `account: "not needed"`, `user: "not needed"` | Same reason — the values are ignored |
| `profiles.yml` inside the project directory | `snow dbt deploy` bundles it from there, not from `~/.dbt/` |

Deploy and run:

```bash
snow dbt deploy DBT_LEARN_PROJECT \
  --source ./dbt_test_snowflake \
  --database DBT_LEARN --schema DEV \
  -c <your-connection>
```

```sql
EXECUTE DBT PROJECT DBT_LEARN.DEV.DBT_LEARN_PROJECT ARGS = 'build';
```

Verified: `Done. PASS=25 WARN=0 ERROR=0 SKIP=0 TOTAL=25` — including the Snowpark Python
model, which does work in a deployed project.

Re-running `snow dbt deploy` with the same name creates `VERSION$2`, `VERSION$3`, … rather
than overwriting.

```sql
SHOW VERSIONS IN DBT PROJECT DBT_LEARN.DEV.DBT_LEARN_PROJECT;
```

> **Note on engine version.** The deployed project runs Snowflake's dbt engine
> (`dbt=1.9.4`, `dbt-snowflake=1.9.2`), which is older than the local pins here
> (`1.12.3` / `1.12.0`). Do not assume the newest dbt features are available server-side.

### Schedule it

```sql
CREATE OR REPLACE TASK DBT_LEARN.DEV.RUN_DBT_DAILY
  WAREHOUSE = <your_wh>
  SCHEDULE  = 'USING CRON 0 6 * * * UTC'
AS
EXECUTE DBT PROJECT DBT_LEARN.DEV.DBT_LEARN_PROJECT ARGS = 'build';

ALTER TASK DBT_LEARN.DEV.RUN_DBT_DAILY RESUME;
```

### Known limitation: `env.yml` needs CLI ≥ 3.22

`env.yml` is how a deployed project switches between dev and prod targets. It is **not**
supported by Snowflake CLI 3.14 — that version validates `profiles.yml` textually and
rejects Jinja outright:

```
Found following errors in profiles.yml:
 * Role '{{ env_var('DBT_CURRENT_ROLE') }}' does not exist or is not accessible.
```

A ready-to-use file ships here as `dbt_test_snowflake/env.yml.needs-cli-3.22`. To enable it:

```bash
snow --version                                    # confirm >= 3.22
cd dbt_test_snowflake
mv env.yml.needs-cli-3.22 env.yml
# then switch profiles.yml back to env_var('DBT_CURRENT_ROLE') etc.
```

Naming rules are enforced and a violation fails the run: every key under `env:` must be
**UPPERCASE** and **`DBT_`-prefixed**; secrets go under `secrets:` with a
`DBT_ENV_SECRET_` prefix and must reference a Snowflake `SECRET` object — never a
plaintext value.

```bash
snow dbt deploy DBT_LEARN_PROJECT --source ./dbt_test_snowflake \
  --database DBT_LEARN --schema DEV --default-env dev -c <your-connection>

snow dbt execute --env prod DBT_LEARN_PROJECT run
```

```sql
EXECUTE DBT PROJECT DBT_LEARN.DEV.DBT_LEARN_PROJECT
  ARGS = 'run' ENVIRONMENT = 'prod';
```

---

## What gets created in Snowflake

| Object | Type | Built by |
|---|---|---|
| `DBT_LEARN.DEV_STAGING.STG_ORDERS` | view | dbt SQL model |
| `DBT_LEARN.DEV_STAGING.STG_CUSTOMER` | view | dbt SQL model |
| `DBT_LEARN.DEV_MARTS.FCT_CUSTOMER_ORDERS` | table | dbt SQL model |
| `DBT_LEARN.DEV_MARTS.FCT_DAILY_ORDERS` | incremental table | dbt SQL model |
| `DBT_LEARN.DEV_MARTS.PY_CUSTOMER_SEGMENTS` | table | dbt **Python** model (Snowpark) |
| `DBT_LEARN.DEV_MARTS.SPARK_MONTHLY_GROWTH` | table | **PySpark** via Snowpark Connect |
| `DBT_LEARN.DEV.DBT_LEARN_PROJECT` | dbt project | `snow dbt deploy` |

---

## Troubleshooting summary

| Symptom | Cause | Fix |
|---|---|---|
| `Database 'SNOWFLAKE_SAMPLE_DATA' does not exist or not authorized` | Role has no privileges on the share | `GRANT IMPORTED PRIVILEGES ON DATABASE snowflake_sample_data TO ROLE <role>` |
| `290404 (08001): 404 Not Found ... login-request` | `account` missing region | Use `ACCT.us-east-1` |
| `AssertionError` at `_invoke_function` | Classic `pyspark.sql.functions` under a Connect session | Import from `pyspark.sql.connect` |
| `SCOS 5001: 'Session' object has no attribute '_spark_session_cache_registry'` | Pre-created a Snowpark session before `start_session()` | Let SCOS own it via `SNOWFLAKE_CONNECTION_NAME` |
| `RuntimeError: cannot schedule new futures after shutdown` | gRPC teardown race after `main()` | Benign — ignore |
| `Role '{{ env_var(...) }}' does not exist` on deploy | CLI < 3.22 cannot resolve Jinja in `profiles.yml` | Use literals, or upgrade the CLI |
| `250001: Could not connect to Snowflake backend` / `RuntimeError: Snowpark Connect session failed to start` | The named connection's `host` is unreachable (locator-form hosts are often stale) | Point `SNOWFLAKE_CONNECTION_NAME` at a connection whose host resolves, or drop the explicit `host` and let the account identifier drive it |
| Incremental model still has bad rows after a fix | Normal runs only touch new rows | `dbt run --full-refresh` |
| `PARSE_SYNTAX_ERROR` / empty SQL sent to SCOS server | pyspark 4.x installed | Pin `pyspark==3.5.5` |

---

## Security notes

- `.gitignore` excludes `.env`, `*.p8`, `*.pem`, `*.key`, and `connections.toml`.
- `dbt_test/profiles.yml` contains only `env_var()` calls — no literal credentials.
- Prefer key-pair auth over passwords. Never put a passphrase in a committed file.
- For secrets in a deployed project, use Snowflake `SECRET` objects via the `secrets:`
  block in `env.yml`, not plaintext `env:` values.

---

## References

- [dbt on Snowflake (native projects)](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake)
- [dbt Python models](https://docs.getdbt.com/docs/build/python-models)
- [Snowpark Connect for Spark](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-overview)
- [Snowpark Python DataFrame API](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/index)
