# MinIO (Local Object Storage)

This folder contains the configuration to run **MinIO** locally as a replacement for GCS.  
It provides S3-compatible object storage to hold raw and processed data for the CityPulse platform.

---

## Run MinIO

Start MinIO with Docker Compose:

```bash
docker compose -f platform/minio/docker-compose.yml up -d
