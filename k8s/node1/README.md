# Node 1 - Master + Infra

Node 1 chay cac thanh phan infra tu `docker-compose.yml`:

- local Docker registry
- MinIO data lake
- Iceberg REST catalog
- Postgres metadata rieng cho Iceberg

Mac dinh manifest pin pod vao Kubernetes node co hostname `node1`:

```bash
kubectl get nodes -o wide
```

Neu node cua ban khong ten la `node1`, co 2 cach:

```bash
kubectl label node <real-node-name> traffic-node=node1 --overwrite
```

Hoac sua `nodeSelector` trong `node1.yaml`.

Deploy:

```bash
kubectl apply -f k8s/node1/node1.yaml
kubectl -n traffic-infra get pods,svc,pvc
```

Port exposed bang NodePort:

- Registry: `node-ip:30500`
- MinIO S3 API: `node-ip:30900`
- MinIO Console: `node-ip:30901`
- Iceberg REST: `node-ip:30818`
- Iceberg Postgres: chi expose noi bo trong cluster tai `iceberg-postgres.traffic-infra.svc.cluster.local:5432`

Thong tin mac dinh:

- MinIO: `minioadmin` / `minioadmin`
- Iceberg Postgres: `iceberg` / `iceberg`, DB `iceberg`
- Warehouse: `s3://traffic-lake/warehouse`

