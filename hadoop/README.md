# Local Hadoop HDFS setup (Docker Compose)

A single-node HDFS (1 NameNode + 1 DataNode) for developing and demonstrating GridGuard.

> **Testing status — please read.** This setup was written from the published
> `apache/hadoop` Docker Compose pattern. It has **not** been run end-to-end by the author
> of this repository (the build environment had no Docker or network access). Expect to
> verify it yourself with the checks below. The Spark ↔ HDFS code path in `run_pipeline.py`
> is therefore also **not end-to-end tested**.

## Prerequisites

* Docker Desktop (or Docker Engine) with the `docker compose` plugin, ~4 GB free RAM.
* Python environment from the project `requirements.txt` and Java 11/17 (needed by PySpark).
* Check that the image tag exists on your machine: `docker pull apache/hadoop:3.3.6`
  (if the tag is unavailable, pick another 3.x tag from https://hub.docker.com/r/apache/hadoop/tags
  and edit `docker-compose.yml`).

## 1. Start

```bash
cd hadoop
docker compose up -d
docker compose ps          # both services should become "healthy" (30–60 s)
docker compose logs -f namenode   # Ctrl+C to leave the logs
```

Web UI: http://localhost:9870 (Datanodes tab should show 1 live node).

## 2. One-time host setup (hosts file)

The DataNode advertises itself as `datanode`. Your host machine must resolve that name to
the published ports on `localhost`:

* Linux / macOS: `echo "127.0.0.1 namenode datanode" | sudo tee -a /etc/hosts`
* Windows (run Notepad as Administrator): add the line `127.0.0.1 namenode datanode` to
  `C:\Windows\System32\drivers\etc\hosts`

Without this, small metadata operations work but **reading/writing file contents from
the host fails** (Spark: "could not obtain block"; dashboard WebHDFS: redirect error).

## 3. Verify HDFS

```bash
docker exec gridguard-namenode hdfs dfsadmin -report | head -20   # "Live datanodes (1)"
docker exec gridguard-namenode hdfs dfs -ls /
```

## 4. Create the GridGuard directories

`run_pipeline.py --hdfs` creates them automatically. To do it by hand:

```bash
docker exec gridguard-namenode hdfs dfs -mkdir -p \
  /gridguard/raw \
  /gridguard/processed/hourly_consumption \
  /gridguard/processed/daily_consumption \
  /gridguard/processed/pipeline_summary \
  /gridguard/results/detected_anomalies \
  /gridguard/results/peak_analysis/top_periods \
  /gridguard/results/peak_analysis/peak_hour_frequency
docker exec gridguard-namenode hdfs dfs -ls -R /gridguard
```

`daily_consumption` and `pipeline_summary` are small additions to the four directories in
the project brief (the dashboard needs a daily table and run metadata).

## 5. Upload the raw CSV

Option A — let the pipeline do it (uses Spark's Hadoop client):

```bash
python data_download.py
export GRIDGUARD_HDFS_ENABLED=true          # Windows PowerShell: $env:GRIDGUARD_HDFS_ENABLED="true"
python run_pipeline.py --hdfs
```

Option B — manually with the HDFS CLI inside the container:

```bash
docker cp ../data/household_power_consumption.txt gridguard-namenode:/tmp/
docker exec gridguard-namenode hdfs dfs -put -f /tmp/household_power_consumption.txt /gridguard/raw/
docker exec gridguard-namenode hdfs dfs -ls -h /gridguard/raw
```

## 6. Inspect saved outputs

```bash
docker exec gridguard-namenode hdfs dfs -ls -R /gridguard/results
docker exec gridguard-namenode hdfs dfs -cat /gridguard/results/detected_anomalies/part-*.csv | head -5
docker exec gridguard-namenode hdfs dfs -du -h /gridguard
```

## 7. How the code connects to this HDFS

| Component | Setting | Default |
|---|---|---|
| Spark pipeline (`run_pipeline.py`, notebook Mode A) | `GRIDGUARD_HDFS_URI` | `hdfs://localhost:8020` |
| Streamlit dashboard (reads via WebHDFS) | `GRIDGUARD_WEBHDFS_URL` | `http://localhost:9870` |
| Enable HDFS | `GRIDGUARD_HDFS_ENABLED` | `false` |
| HDFS user | `GRIDGUARD_HDFS_USER` | `hadoop` |

The pipeline first checks that the NameNode port answers. If it does not, it **falls back
to local storage and says so** — local output is never labelled as HDFS. When HDFS is used,
the pipeline also writes a local copy to `outputs/` (labelled `local_export_of_hdfs`) so the
dashboard works even if WebHDFS networking is not set up.

## 8. Google Colab vs. a Docker cluster on your laptop

* A Colab VM runs in Google's cloud. `localhost` there is **the Colab VM**, not your laptop,
  so it cannot reach `hdfs://localhost:8020` of your Docker cluster. Colab also has no HDFS
  of its own. That is why the notebook has two explicit modes: **Mode A** (needs a NameNode
  that is reachable from Colab, e.g. a cloud VM with public/VPN-reachable ports 8020, 9866 and a
  hostname the DataNode advertises) and **Mode B** (standalone, local Colab disk).
* Exposing a Hadoop cluster to the internet is a security risk (no authentication in this
  dev setup). For a college demo, prefer running the local pipeline + dashboard against
  Docker on your laptop, and use Colab in Mode B.

## 9. Stop and clean up

```bash
docker compose stop            # stop, keep data
docker compose down            # remove containers, keep HDFS data volumes
docker compose down -v         # remove containers AND all HDFS data (full reset)
```

## Known issues / troubleshooting

* **Healthcheck stays "starting"** – check `docker compose logs namenode`. On first start the
  NameNode must format its directory; if a previous run left a broken volume use `down -v`.
* **Permission denied on the volume** – the image runs as a non-root user. If the NameNode
  cannot write `/tmp/hadoop-root/dfs/name`, check the logs and adjust the volume paths
  to the directories your image tag uses (`docker exec gridguard-namenode hdfs getconf -confKey dfs.namenode.name.dir`).
* **`UnresolvedAddressException` / "could not obtain block"** – the hosts-file entry from step 2 is missing.
* **DataNode not registered** – `docker compose logs datanode`; make sure the NameNode was healthy first.
* The configuration disables HDFS permissions (`dfs.permissions.enabled=false`) and has no
  authentication. **Development use only.**
