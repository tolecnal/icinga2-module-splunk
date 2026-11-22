# Icinga2 to Splunk Integration

Streams monitoring data from Icinga2 to Splunk using the
[Icinga2 API event stream](https://icinga.com/docs/icinga-2/latest/doc/12-icinga2-api/#event-streams)
and the Splunk HTTP Event Collector (HEC).

## Features

- **Real-time streaming** from the Icinga2 `/v1/events` API: no per-check processes.
- **Event types**: StateChange, Notification, Acknowledgement set/cleared, Comment
  added/removed, Downtime added/removed/started/triggered, and optionally raw
  CheckResults. Any other subscribed type (e.g. `ObjectCreated`) is forwarded as is
  (note: generic events are not filtered, so avoid subscribing to object-change types
  if your Icinga2 objects contain secrets in custom variables).
- **Performance data**: each metric becomes its own event, with thresholds, in a
  separate index. Nagios range thresholds (`10:20`, `@0:5`, `~:10`) are kept as strings.
- **Survives Splunk outages**: while Splunk is unreachable, events are written to a
  disk spool limited by age (default 7 days) and size (default 1 GB). The spool is
  replayed, oldest first, once Splunk is reachable again, including after a restart.
  See [Delivery and outages](#delivery-and-outages).
- **Self-healing connection**: dead streams are detected with a read timeout
  (`heartbeat_timeout`), and reconnects use exponential backoff (1 s up to 60 s).
- **Secure defaults**: TLS verification is on and can point at the Icinga2 CA. Check
  command lines are not forwarded (they often contain passwords). The systemd unit is
  sandboxed.

## Architecture

```
Icinga2 API (/v1/events, chunked JSON stream)
        │
EventStreamClient ── converts events, parses perfdata
        │
SplunkHECClient ──── memory queue → batches → /services/collector/event
        │                └── disk spool (/var/lib/icinga2-splunk/spool) during outages
        │
Splunk
 ├── icinga2_events    (state changes, notifications, acks, comments, downtimes)
 └── icinga2_perfdata  (one event per metric)
```

## Requirements

- Python 3.10 or later (on RHEL 9, install `python3.11` or `python3.12` and set `PYTHON`).
- Icinga2 with the API feature enabled.
- Splunk Enterprise or Splunk Cloud with HEC enabled.

## Installation

### Debian and Ubuntu packages

Packages are built for Debian 12 and 13 and for Ubuntu 22.04, 24.04 and 26.04. Each
[release](https://github.com/tolecnal/icinga2-module-splunk/releases) has one `.deb`
per distribution release, e.g. `icinga2-splunk_2.0.0_deb12_all.deb` or
`icinga2-splunk_2.0.0_ubuntu24.04_all.deb`. (The package version inside is
`2.0.0~deb12`; release file names use `_` because GitHub doesn't allow `~`.)

First create the Icinga2 API user and the Splunk HEC token (steps 2 and 3 of the script
install below). Then:

```bash
sudo apt install ./icinga2-splunk_2.0.0_deb12_all.deb
sudoedit /etc/icinga2-splunk/splunk_config.json
sudo -u icinga2-splunk icinga2-splunk-check
sudo systemctl enable --now icinga2-splunk
```

The package differs from the script install below:

| Item              | Package                                   | `install.sh`                       |
| ----------------- | ----------------------------------------- | ---------------------------------- |
| Code              | `/usr/lib/python3/dist-packages`, system `python3-requests` | virtualenv in `/opt/icinga2-splunk` |
| Config            | `/etc/icinga2-splunk/splunk_config.json`  | `/etc/icinga2/splunk_config.json`  |
| Environment file  | `/etc/default/icinga2-splunk`             | `/etc/icinga2/splunk.env`          |
| Service user      | `icinga2-splunk` (created by the package) | `nagios` (or `SERVICE_USER`)       |

The service isn't enabled on install because it needs configuring first. Once you
enable it, package upgrades restart it. `apt purge` also deletes the disk spool.

Because the service runs as its own user, it can't read Icinga2's private
`/var/lib/icinga2/certs` directory. Copy the CA certificate (and any client
certificate) somewhere it can read, for example:

```bash
sudo install -m 0644 /var/lib/icinga2/certs/ca.crt /etc/icinga2-splunk/ca.crt
```

Then set `"icinga2_api_verify_ssl": "/etc/icinga2-splunk/ca.crt"`.

### Script install (any distribution)

#### 1. Install the package and service

```bash
git clone https://github.com/tolecnal/icinga2-module-splunk.git
cd icinga2-module-splunk
sudo ./scripts/install.sh
```

The installer:

- creates a virtualenv in `/opt/icinga2-splunk` and installs the package into it,
- creates `/etc/icinga2/splunk_config.json` (mode `0640`, group = service user) if it
  doesn't exist yet,
- installs and reloads the `icinga2-splunk` systemd unit.

You can override these settings with environment variables: `PREFIX`, `PYTHON`,
`SERVICE_USER` (default `nagios`; use `icinga` on RHEL/SUSE) and `CONFIG_FILE`.
Example:

```bash
sudo SERVICE_USER=icinga PYTHON=python3.12 ./scripts/install.sh
```

#### 2. Create an Icinga2 API user

```
object ApiUser "splunk" {
  password = "your-secure-password"
  permissions = [ "events/*", "status/query" ]
}
```

`status/query` is only used by `icinga2-splunk-check`. You can limit `events/*` to the
types you subscribe to, e.g. `events/statechange` and `events/checkresult`. Restart
Icinga2 after you add the user.

#### 3. Configure Splunk HEC

1. Go to **Settings > Data Inputs > HTTP Event Collector** and create a token.
2. Allow the `icinga2_events` and `icinga2_perfdata` indexes for the token.
3. Create the indexes under **Settings > Indexes** if they don't exist.

#### 4. Configure the integration

Edit `/etc/icinga2/splunk_config.json`. See
[`config.json.example`](config.json.example) for all options.

```json
{
  "icinga2_api_url": "https://icinga2.example.com:5665",
  "icinga2_api_username": "splunk",
  "icinga2_api_password": "your-secure-password",
  "icinga2_api_verify_ssl": "/var/lib/icinga2/certs/ca.crt",

  "splunk_hec_url": "https://splunk.example.com:8088",
  "splunk_hec_token": "your-hec-token"
}
```

If you don't want secrets in the JSON file, put them in `/etc/icinga2/splunk.env`
(mode `0600`, owned by root) instead. The systemd unit loads this file, for example:

```
ICINGA2_API_PASSWORD=your-secure-password
SPLUNK_HEC_TOKEN=your-hec-token
```

#### 5. Test and start

```bash
sudo -u nagios /opt/icinga2-splunk/bin/icinga2-splunk-check
sudo systemctl enable --now icinga2-splunk
journalctl -u icinga2-splunk -f
```

## Configuration reference

You can set every option in the JSON file. Environment variables override the file.
Set `ICINGA2_SPLUNK_CONFIG` or pass `--config` to use a different file.

### Icinga2

| Option                    | Env variable              | Default  | Description                                       |
| ------------------------- | ------------------------- | -------- | ------------------------------------------------- |
| `icinga2_api_url`         | `ICINGA2_API_URL`         | required | e.g. `https://localhost:5665`                     |
| `icinga2_api_username`    | `ICINGA2_API_USERNAME`    | –        | API user (or use a client certificate)            |
| `icinga2_api_password`    | `ICINGA2_API_PASSWORD`    | –        |                                                   |
| `icinga2_api_client_cert` | `ICINGA2_API_CLIENT_CERT` | –        | Client certificate path (alternative auth)        |
| `icinga2_api_client_key`  | `ICINGA2_API_CLIENT_KEY`  | –        | Client key path                                   |
| `icinga2_api_verify_ssl`  | `ICINGA2_API_VERIFY_SSL`  | `true`   | `true`/`false`, or a CA bundle path (recommended) |

### Event stream

| Option                      | Env variable                | Default                | Description                                         |
| --------------------------- | --------------------------- | ---------------------- | --------------------------------------------------- |
| `event_types`               | `EVENT_TYPES` (comma list)  | see example            | Icinga2 event types to subscribe to                 |
| `event_stream_queue`        | `EVENT_STREAM_QUEUE`        | `splunk`               | Queue name; instances with the same name share load |
| `event_stream_filter`       | `EVENT_STREAM_FILTER`       | `""`                   | Icinga2 filter expression                           |
| `include_perfdata`          | `INCLUDE_PERFDATA`          | `true`                 | Send perfdata (subscribes to `CheckResult`)         |
| `include_check_results`     | `INCLUDE_CHECK_RESULTS`     | `false`                | Also send CheckResult events                        |
| `check_result_state_filter` | `CHECK_RESULT_STATE_FILTER` | `[1, 2, 3]`            | CheckResult states to send; `[]` sends all          |
| `heartbeat_timeout`         | `HEARTBEAT_TIMEOUT`         | `300`                  | Reconnect if no data arrives for this many seconds (see below) |

Icinga2 sends nothing on the event stream while no subscribed events happen, so the
client can't tell a quiet stream from a dead connection. After `heartbeat_timeout`
seconds without data it reconnects immediately (logged at INFO). Events that occur
during the short resubscribe are missed. With perfdata enabled there is a steady
flow of CheckResult events and this rarely happens. If you subscribe only to rare
event types, raise `heartbeat_timeout` (e.g. to `3600`).

### Splunk

| Option                  | Env variable            | Default            | Description                                   |
| ----------------------- | ----------------------- | ------------------ | --------------------------------------------- |
| `splunk_hec_url`        | `SPLUNK_HEC_URL`        | required           | e.g. `https://splunk.example.com:8088`        |
| `splunk_hec_token`      | `SPLUNK_HEC_TOKEN`      | required           |                                               |
| `splunk_verify_ssl`     | `SPLUNK_VERIFY_SSL`     | `true`             | `true`/`false`, or a CA bundle path           |
| `splunk_index_events`   | `SPLUNK_INDEX_EVENTS`   | `icinga2_events`   |                                               |
| `splunk_index_perfdata` | `SPLUNK_INDEX_PERFDATA` | `icinga2_perfdata` |                                               |
| `batch_size`            | `BATCH_SIZE`            | `100`              | Events per HEC request                        |
| `batch_timeout`         | `BATCH_TIMEOUT`         | `10`               | Maximum seconds before a partial batch is sent |
| `max_queue_size`        | `MAX_QUEUE_SIZE`        | `10000`            | Events held in memory (see below)             |
| `retry_max_interval`    | `RETRY_MAX_INTERVAL`    | `60`               | Maximum seconds between retries during an outage |
| `log_level`             | `LOG_LEVEL`             | `INFO`             | `DEBUG`, `INFO`, `WARNING`, `ERROR`           |

### Spool

| Option               | Env variable         | Default                         | Description                                 |
| -------------------- | -------------------- | ------------------------------- | ------------------------------------------- |
| `spool_dir`          | `SPOOL_DIR`          | `/var/lib/icinga2-splunk/spool` | Absolute path; `""` disables the disk spool |
| `spool_max_size_mb`  | `SPOOL_MAX_SIZE_MB`  | `1024`                          | Oldest events are discarded beyond this     |
| `spool_max_age_days` | `SPOOL_MAX_AGE_DAYS` | `7`                             | Spooled events older than this are discarded |

The systemd unit creates the default spool directory (`StateDirectory=`). If you run the
service another way, make sure the directory is writable by the service user. If it
isn't, the service exits with code 78.

A `splunk_hec_url` that ends in `/services/collector` or `/services/collector/event` is
also accepted.

## Delivery and outages

The client sorts every HEC response into one of three groups:

| Response                                                                 | Meaning           | Action                                                                                         |
| ------------------------------------------------------------------------ | ----------------- | ---------------------------------------------------------------------------------------------- |
| 2xx                                                                      | Delivered         | –                                                                                              |
| 400 with a malformed-event code (6, 12, 13, 15–21), 413 too large       | Bad data          | Only the offending event is discarded and logged. The batch is split to find it if needed.    |
| Connection error, timeout, 3xx, 429, 5xx, 401, 403, 404, any other 400 (e.g. "Incorrect index") | Splunk unavailable | Nothing is discarded. Events go to the spool, and delivery is retried with backoff from 1 s up to `retry_max_interval`. |

Redirects, authentication errors and other unexpected 400 responses count as
"unavailable" rather than "bad data". They are usually configuration problems an
admin can fix, and the events should survive until then. They are logged at ERROR
level. Redirects are never followed, so the HEC token can't be sent to another host;
set `splunk_hec_url` to the final URL.

Because these responses block delivery, a batch that Splunk keeps rejecting with an
unexpected 400 holds back all later events until the cause is fixed or the spool
limits are reached. Watch for "Could not deliver" errors in the log.

When Splunk is reachable again, the spool is replayed oldest first, then live events
follow. During replay, new events are moved to the spool so memory stays bounded.
Delivery is at-least-once: if the process crashes during replay, part of a segment may
be sent twice.

Events are discarded only when:

- the spool exceeds `spool_max_size_mb` or `spool_max_age_days` (oldest first, logged
  at ERROR level),
- the spool is disabled (`spool_dir: ""`) and more than `max_queue_size` events are
  waiting in memory, or the service stops during an outage,
- Splunk rejects a single event as malformed.

## Splunk events

| Sourcetype                | Index               | Notable fields                                                       |
| ------------------------- | ------------------- | -------------------------------------------------------------------- |
| `icinga2:statechange`     | events              | `state`, `state_name`, `state_type_name`, `previous_state_name`, `output` |
| `icinga2:notification`    | events              | `notification_type`, `users`, `author`, `text`, `state_name`         |
| `icinga2:acknowledgement` | events              | `author`, `comment`, `acknowledgement_type`, `expiry`                |
| `icinga2:comment`         | events              | `author`, `text`, `entry_type`                                       |
| `icinga2:downtime`        | events              | `author`, `comment`, `start_time`, `end_time`, `fixed`               |
| `icinga2:checkresult`     | events              | `state_name`, `output`, `exit_status`, `check_source`                |
| `icinga2:perfdata`        | perfdata            | `metric`, `value`, `unit`, `warning`, `critical`, `min`, `max`       |

All events include `event_type`, `host`, `service`, `object_name` (`host!service`)
and `timestamp`.

```spl
index=icinga2_events sourcetype=icinga2:statechange state_type_name=HARD
| table _time host service state_name previous_state_name output

index=icinga2_perfdata host=web1 metric=rta
| timechart avg(value) by service
```

## Upgrading from 1.x

- The per-check `event_handler.py` and `perfdata_handler.py` scripts and
  `icinga2_config/splunk.conf` were removed. They did not work: the config used invalid
  `apply EventCommand` syntax, perfdata failed on a missing method, and batched events
  were lost when the process exited. The event stream service replaces them.
- The package is now installed into a virtualenv. Re-run `scripts/install.sh`, then
  remove the old `/usr/local/bin/icinga2_splunk` directory.
- `test_connection.py` is now the `icinga2-splunk-check` command.
- Perfdata is no longer sent twice (once from `StateChange`, once from `CheckResult`).
- Comment and downtime events now have the correct `host` and `service`.
- `previous_state` now holds the real previous state (it was always `0`).
- The check `command` field is no longer included in CheckResult events, because
  command lines often contain credentials.
- `max_retries` was removed. Delivery now retries until the spool limits are reached.
  The key is ignored with a warning.
- The deprecated keys `event_stream_types`, `splunk_batch_size` and
  `splunk_batch_interval` are still accepted and log a warning.

## Development

```bash
make dev     # create .venv, install dev dependencies and pre-commit hooks
make check   # ruff lint + format check, mypy --strict, pytest with coverage, all hooks
make format  # auto-fix lint issues and reformat
```

Tools: [ruff](https://docs.astral.sh/ruff/) (linting, including bandit security rules,
and formatting), mypy (strict), pytest with 100% line and branch coverage (enforced), shellcheck and
gitleaks (through pre-commit). CI runs the same checks on Python 3.10–3.14. It also
builds and smoke-tests the wheel and runs `pip-audit`.

### Building packages

```bash
make deb                                 # all five releases, in Docker
make deb IMAGES="debian:12 ubuntu:22.04" # selected releases
```

`packaging/docker-build.sh` builds each package in a container of the target release.
It runs the test suite and lintian during the build. It then installs, checks and
purges the package in a fresh container. The packages are written to `dist/deb/`.

The **Debian packages** workflow does the same on every push and pull request. When a
`v*` tag is pushed, it attaches the packages to the GitHub release. For a release:

1. Bump `__version__` in `src/icinga2_splunk/__init__.py`.
2. Add a matching `debian/changelog` entry (`dch -v X.Y.Z`). The build fails if the two
   versions differ.
3. Merge, then tag `vX.Y.Z` and push the tag.

The `icinga2_config/features-available/splunk.conf` file is an unrelated alternative
setup. It writes perfdata files that a Splunk Universal Forwarder can monitor.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE).
