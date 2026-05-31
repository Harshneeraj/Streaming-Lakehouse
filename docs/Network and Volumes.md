# Docker Networking and Volumes

## Network: Bridge

All containers share a single Docker bridge network called `hudi-net`.

**What a bridge does:**
- Every container gets its own IP address.
- Containers find each other by name (`kafka`, `minio`, `hive-metastore`) via Docker's built-in DNS.
- Traffic between containers stays inside the bridge — never leaves the host.
- Outside world can only reach containers through explicit port mappings.

**Why bridge and not other options:**

| Driver | What it does | Why not for us |
|--------|-------------|----------------|
| `bridge` | Private virtual switch, DNS between containers | ✅ This is what we use |
| `host` | Container shares the host's network directly | Port conflicts between services |
| `none` | No network at all | Containers need to talk to each other |
| `overlay` | Spans multiple physical machines | We're on one machine |

---

## Volumes

Volumes are how data survives container restarts.

### Named volumes (persistent data)

```yaml
volumes:
  kafka-data:       # Kafka's message logs
  minio-data:       # All S3 objects (Hudi Parquet files)
  postgres-data:    # Hive Metastore's catalog tables
  ivy-cache:        # Spark's dependency cache
  hms-aux-libs:     # Postgres JDBC jar for HMS
  hms-logs:         # HMS log files
```

Example: `minio-data:/data` means "mount the volume at `/data` inside the container."

### Bind mounts (config files and source code)

Example: `./spark/conf/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf:ro`

Maps a file from your project into the container. `:ro` = read-only.

### Lifecycle

```bash
make down    # stops containers, volumes stay (data preserved)
make clean   # stops containers AND deletes volumes (full reset)
```

---

## Ivy Cache

A folder where Spark stores downloaded jars when you use `--packages`. In our project we baked all jars into the image, so this is just a safety net. Mounted as a volume so any incidental downloads persist.
