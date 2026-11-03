# Kubernetes Metrics Pipeline on AWS EKS

A highly available, long-retention metrics platform on Amazon EKS. Two Prometheus replicas scrape the same targets, Thanos provides a unified query layer and long-term storage in Amazon S3, and Grafana visualizes everything through a single endpoint.

Built as an SRE portfolio project to demonstrate production-style observability: HA monitoring, durable long-term storage, least-privilege AWS access, and verified failure recovery.

---
![Kubernetes](https://img.shields.io/badge/Kubernetes-1.37-326CE5?logo=kubernetes&logoColor=white)
![AWS EKS](https://img.shields.io/badge/AWS-EKS-FF9900?logo=amazonaws&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-HA%20x2-E6522C?logo=prometheus&logoColor=white)
![Thanos](https://img.shields.io/badge/Thanos-v0.38.0-7B3FE4)
![Grafana](https://img.shields.io/badge/Grafana-dashboards-F46800?logo=grafana&logoColor=white)
![S3](https://img.shields.io/badge/Amazon%20S3-long--term%20storage-569A31?logo=amazons3&logoColor=white)
![Helm](https://img.shields.io/badge/Helm-kube--prometheus--stack-0F1689?logo=helm&logoColor=white)

## Table of contents

1. [The problem](#the-problem)
2. [Architecture](#architecture)
3. [Why each design decision was made](#why-each-design-decision-was-made)
4. [Repository layout](#repository-layout)
5. [Prerequisites](#prerequisites)
6. [Placeholders used in this repo](#placeholders-used-in-this-repo)
7. [Step-by-step deployment](#step-by-step-deployment)
8. [Verification and test results](#verification-and-test-results)
9. [Troubleshooting](#troubleshooting)
10. [Security notes](#security-notes)
11. [Cost](#cost)
12. [Teardown and rebuild](#teardown-and-rebuild)
13. [Possible next steps](#possible-next-steps)

---

## The problem

A single Prometheus server on Kubernetes has four limitations that matter in production:

| Problem | What it means in practice |
|---|---|
| **Limited retention** | Prometheus stores data on local disk. Keeping months of history means large, expensive volumes that are hard to resize. |
| **Single point of failure** | If the one Prometheus pod restarts or its node fails, metrics stop and gaps appear in dashboards and alerts. |
| **No global view** | Each Prometheus only knows its own data. Querying across replicas, clusters, or regions needs manual work. |
| **Fragile backups** | Copying TSDB files around is error-prone, and restoring them is slow. |

This project solves all four by separating **collection** (Prometheus), **storage** (S3), and **query** (Thanos). Each layer can fail or scale independently.

---

## Architecture
<img width="1384" height="824" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/640abee0-b47c-4b60-aa17-67302b879f65" />




### Data flow

1. Both Prometheus replicas scrape the same targets independently, so each series is recorded twice. This is deliberate: if one replica is down, the other still has the data.
2. Each Thanos sidecar exposes its replica's recent data over gRPC and uploads completed 2-hour blocks to S3.
3. The Store Gateway reads blocks from S3 and serves historical queries.
4. The Compactor (a single instance) merges blocks, applies retention, and downsamples old data.
5. Thanos Query fans each request out to the sidecars and the Store Gateway, then removes duplicate series from the two replicas using the `prometheus_replica` label.
6. Grafana connects only to Thanos Query, so it sees one continuous timeline regardless of where the data lives.

### Components

| Component | Runs as | Why it's here |
|---|---|---|
| Prometheus (x2) | StatefulSet via Prometheus Operator | Collects metrics. Two replicas remove the single point of failure. |
| Thanos sidecar | Container in each Prometheus pod | Uploads blocks to S3 and exposes live data to Query. Prometheus itself is unchanged. |
| Thanos Query | Deployment (x2) | One query endpoint that merges and deduplicates all sources. Stateless, so it scales easily. |
| Thanos Store Gateway | StatefulSet (x1) | Serves historical data straight from S3, so Prometheus does not need months of local data. |
| Thanos Compactor | StatefulSet (x1, must stay at 1) | Keeps S3 tidy: merges small blocks, downsamples old data, enforces retention. |
| Grafana | Deployment via kube-prometheus-stack | Dashboards for humans. Reads from Thanos Query only. |
| Alertmanager | StatefulSet via kube-prometheus-stack | Receives alerts from Prometheus. |
| Amazon S3 | Private, encrypted, versioned bucket | Durable, cheap long-term storage that is independent of the cluster. |
| IRSA | IAM role bound to Kubernetes service accounts | Gives pods S3 access without static AWS keys. |

---

## Why each design decision was made

**Two Prometheus replicas instead of one.** A single instance means a pod restart creates a gap in every graph and alert. Two replicas scraping the same targets keep data flowing when one fails. The cost is double the live storage, which stays small because S3 holds the history.

**Thanos sidecar instead of remote write.** The sidecar uploads Prometheus's own blocks and needs no change to how Prometheus collects data. Remote write sends every sample over the network as it arrives, and it does not give you the replica-aware deduplication Thanos Query provides.

**Amazon S3 for history.** S3 is durable, cheap per GB, and decoupled from the cluster. You can delete the entire EKS cluster and still query the old blocks after rebuilding it. This is the property that makes the design resilient.

**Store Gateway instead of local history.** Without it, historical queries would need data on Prometheus's disk. The Store Gateway reads only the parts of S3 a query needs, and it keeps a small index cache.

**A single Compactor.** Compaction rewrites blocks in S3. Two compactors working on the same bucket can corrupt it, so the StatefulSet stays at one replica.

**IRSA instead of access keys.** Each component gets short-lived credentials through its own service account, scoped to one bucket. No `AWS_ACCESS_KEY_ID` exists in the cluster or in git.

**Spot instances for worker nodes.** This is a lab, and the design tolerates node loss. Pods reschedule, EBS volumes reattach, and the S3 data is unaffected. Spot pricing makes the project cheap enough to rebuild often.

**Grafana password in a Kubernetes secret.** The password never appears in a values file, so the repo contains no credentials.

---

## Repository layout

```text
obs-eks/
├── eks/
│   └── cluster.yaml                 # eksctl cluster: VPC, Spot nodes, OIDC, addons
├── helm/
│   └── kps-values.yaml              # kube-prometheus-stack: Prometheus, sidecars, Grafana
├── iam/
│   ├── thanos-s3-policy.json        # Least-privilege S3 policy for one bucket
│   ├── lifecycle.json               # Expires old noncurrent S3 object versions
│   └── objstore.yml                 # Thanos object storage config (bucket details, no keys)
├── k8s/
│   ├── storageclass-gp3.yaml        # Default gp3 StorageClass
│   ├── sidecar-discovery.yaml       # Headless service for sidecar gRPC discovery
│   ├── thanos-store.yaml            # Store Gateway StatefulSet and headless service
│   ├── thanos-compactor.yaml        # Compactor StatefulSet and service
│   ├── thanos-query.yaml            # Thanos Query Deployment and service
│   ├── thanos-servicemonitor.yaml   # Scrapes Query, Store, and Compactor metrics
│   ├── sample-app.yaml              # podinfo test workload and its ServiceMonitor
│   └── alerts.yaml                  # PrometheusRule for pipeline health
├── docs/                            # Screenshots used in this README (optional)
└── README.md
```

---

## Prerequisites

**Tools:** AWS CLI v2, `eksctl` 0.200 or later, `kubectl` within one minor version of the cluster, Helm 3.14 or later, `git`, `jq`.

**AWS access:** A sandbox account where your IAM user can create EKS, EC2, IAM, CloudFormation, Auto Scaling, and S3 resources. For a lab, attaching `AdministratorAccess` to your user is the simplest option. Do not do this in a shared company account without approval.

Creating the cluster failed several times during this project because the IAM user lacked specific permissions, including `ec2:AllocateAddress` for the NAT gateway Elastic IP and `iam:CreateServiceLinkedRole` for EKS. When creation fails with an `AccessDenied` message, the message names the missing action. Add that action to the policy and retry.

**Budget:** Set a monthly budget alert in AWS Billing (for example $30, with an email alert at 80%) before you create anything.

---

## Placeholders used in this repo

| Placeholder | Meaning | Where it appears |
|---|---|---|
| `<ACCOUNT_ID>` | Your 12-digit AWS account ID | Bucket name, IAM policy ARN, role ARNs |
| `<BUCKET_NAME>` | `thanos-metrics-store-<ACCOUNT_ID>` | `iam/objstore.yml`, `iam/thanos-s3-policy.json` |
| `<PROMETHEUS_ROLE_ARN>` | IRSA role ARN for `prometheus-sa` | `helm/kps-values.yaml` |

Set these as shell variables so the commands work unchanged:

```bash
export AWS_REGION=ap-south-1
export CLUSTER=obs-eks
export NS=monitoring
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET=thanos-metrics-store-${ACCOUNT_ID}
export POLICY_ARN=arn:aws:iam::${ACCOUNT_ID}:policy/ThanosS3Access-${CLUSTER}
```

Before committing, run `grep -rn "$ACCOUNT_ID" .` from the repo root and replace any matches with the placeholders.

---

## Step-by-step deployment

Work through the phases in order. Each phase ends with a verification step. Do not move on until it passes.

### Phase 0: Environment

1. Configure credentials and confirm your identity:

```bash
aws configure            # or: aws configure sso
aws sts get-caller-identity
```

2. Save the variables above in `env.sh` and load them in every new terminal with `source env.sh`.

**Verify:** `echo $BUCKET` prints the bucket name with your account ID.

### Phase 1: EKS cluster

3. Check the Kubernetes versions available in your region:

```bash
aws eks describe-cluster-versions --region $AWS_REGION --query 'clusterVersions[0].clusterVersion' --output text
```

4. Review `eks/cluster.yaml`. It defines the cluster name, region, Kubernetes version, OIDC, Auto Mode disabled, a single NAT gateway, the required addons (including the EBS CSI driver), and a Spot managed node group. Set `version` to the value from step 3 if it differs.

5. Create the cluster. This takes 15 to 20 minutes:

```bash
eksctl create cluster -f eks/cluster.yaml
```

6. Connect kubectl and confirm your IAM identity has Kubernetes access:

```bash
aws eks update-kubeconfig --name $CLUSTER --region $AWS_REGION
aws eks list-access-entries --cluster-name $CLUSTER --region $AWS_REGION
```

If your identity is missing from the list, create an access entry and attach the admin policy:

```bash
aws eks create-access-entry --cluster-name $CLUSTER --region $AWS_REGION \
  --principal-arn arn:aws:iam::${ACCOUNT_ID}:user/<YOUR_IAM_USER>

aws eks associate-access-policy --cluster-name $CLUSTER --region $AWS_REGION \
  --principal-arn arn:aws:iam::${ACCOUNT_ID}:user/<YOUR_IAM_USER> \
  --policy-arn arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy \
  --access-scope type=cluster
```

If the create call returns "resource already in use", the entry already exists. Run only the associate step.

**Verify:** `kubectl get nodes` shows two nodes in `Ready` state.

### Phase 2: Storage

7. Apply the default `gp3` StorageClass from `k8s/storageclass-gp3.yaml`, after removing the default flag from `gp2`:

```bash
kubectl patch storageclass gp2 -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
kubectl apply -f k8s/storageclass-gp3.yaml
```

**Verify:** `kubectl get storageclass` shows `gp3 (default)` and no other default.

### Phase 3: S3 bucket

8. Create the bucket with public access blocked and default encryption:

```bash
aws s3api create-bucket --bucket $BUCKET --region $AWS_REGION \
  --create-bucket-configuration LocationConstraint=$AWS_REGION

aws s3api put-public-access-block --bucket $BUCKET --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-encryption --bucket $BUCKET --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

9. Enable versioning and apply the lifecycle rule from `iam/lifecycle.json`:

```bash
aws s3api put-bucket-versioning --bucket $BUCKET --versioning-configuration Status=Enabled
aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET --lifecycle-configuration file://iam/lifecycle.json
```

**Verify:** `aws s3api get-public-access-block --bucket $BUCKET` shows all four settings as `true`.

### Phase 4: IAM policy and IRSA

10. Replace `<BUCKET_NAME>` in `iam/thanos-s3-policy.json` with your bucket name, then create the policy:

```bash
aws iam create-policy --policy-name ThanosS3Access-${CLUSTER} \
  --policy-document file://iam/thanos-s3-policy.json
```

11. Create the namespace and two IRSA service accounts bound to the policy:

```bash
kubectl create namespace $NS

eksctl create iamserviceaccount --cluster $CLUSTER --region $AWS_REGION \
  --namespace $NS --name prometheus-sa --attach-policy-arn $POLICY_ARN --approve

eksctl create iamserviceaccount --cluster $CLUSTER --region $AWS_REGION \
  --namespace $NS --name thanos-sa --attach-policy-arn $POLICY_ARN --approve
```

12. Record the Prometheus role ARN. You need it for the Helm values:

```bash
kubectl -n $NS get sa prometheus-sa -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}'; echo
```

**Verify:** the command prints a role ARN.

### Phase 5: Secrets

13. Replace `<BUCKET_NAME>` in `iam/objstore.yml` with your bucket name, then create the object storage secret. The file contains bucket details only, never credentials:

```bash
kubectl -n $NS create secret generic thanos-objstore-config \
  --from-file=objstore.yml=iam/objstore.yml
```

14. Create the Grafana admin secret with a generated password:

```bash
kubectl -n $NS create secret generic grafana-admin \
  --from-literal=admin-user=admin \
  --from-literal=admin-password="$(openssl rand -base64 24)"
```

Store the password in your password manager. Retrieve it later with:

```bash
kubectl -n $NS get secret grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d; echo
```

**Verify:** decode the object storage secret and confirm the bucket and region are correct:

```bash
kubectl -n $NS get secret thanos-objstore-config -o jsonpath='{.data.objstore\.yml}' | base64 -d; echo
```

### Phase 6: Prometheus, Grafana, and Alertmanager

15. Add the Helm repository:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

16. Open `helm/kps-values.yaml` and set `<PROMETHEUS_ROLE_ARN>` to the value from step 12. Check the three settings that caused problems during the build:
    - `thanosService` and `thanosServiceMonitor` sit at the `prometheus:` level, not under `prometheusSpec`.
    - `objectStorageConfig` nests `name` and `key` under `existingSecret`.
    - Grafana uses `admin.existingSecret`, with no plaintext password.

Confirm no password is in the file:

```bash
grep -n -i "password" helm/kps-values.yaml
```

The only match should be the `passwordKey` line.

17. Install the chart:

```bash
helm upgrade --install kps prometheus-community/kube-prometheus-stack \
  -n $NS -f helm/kps-values.yaml --wait --timeout 15m
```

18. Check the Thanos wiring, not only that the pods are running:

```bash
kubectl -n $NS get prometheus kps-prometheus -o jsonpath='{.spec.thanos}'; echo
kubectl -n $NS get pod prometheus-kps-prometheus-0 \
  -o jsonpath='{.spec.containers[?(@.name=="thanos-sidecar")].args}'; echo
```

The first output must include `objectStorageConfig`. The second must include `--objstore.config-file`.

**Verify:** `kubectl -n $NS get pods` shows `prometheus-kps-prometheus-0` and `-1` at `3/3 Running`, and `alertmanager-kps-alertmanager-0` at `2/2 Running`.

### Phase 7: Thanos components

19. Confirm the sidecar pod labels, since the `sidecar-discovery.yaml` selector must match them:

```bash
kubectl -n $NS get pods prometheus-kps-prometheus-0 --show-labels
```

20. Apply the Thanos manifests. Each StatefulSet in `thanos-store.yaml` and `thanos-compactor.yaml` must include `securityContext.fsGroup: 65534` on its pod spec. The Compactor must stay at one replica.

```bash
kubectl apply -f k8s/sidecar-discovery.yaml
kubectl apply -f k8s/thanos-store.yaml
kubectl apply -f k8s/thanos-compactor.yaml
kubectl apply -f k8s/thanos-query.yaml
```

Thanos Query discovers the sidecars and the Store Gateway through DNS SRV records. It does not list the Compactor, because the Compactor serves no query data.

**Verify:** `kubectl -n $NS get pods` shows `thanos-store-gateway-0`, `thanos-compactor-0`, and both `thanos-query` pods at `1/1 Running`, with no restarts. A `permission denied` error in the logs means `fsGroup` is missing from that pod spec.

### Phase 8: Sample workload and monitoring of the monitoring

21. Deploy the test workload. It runs three `podinfo` pods that expose Prometheus metrics on port 9898, plus a ServiceMonitor:

```bash
kubectl apply -f k8s/sample-app.yaml
```

22. Apply the ServiceMonitor that scrapes Query, the Store Gateway, and the Compactor:

```bash
kubectl apply -f k8s/thanos-servicemonitor.yaml
```

23. Apply the alert rules. Each expression uses `or vector(0)` so that "no data" counts as a failure instead of silence:

```bash
kubectl apply -f k8s/alerts.yaml
```

**Verify in the Prometheus UI.** Port-forward with `kubectl -n $NS port-forward svc/kps-prometheus 9090:9090`, then open `http://localhost:9090`:

- Run `up{job="thanos-query"}`, `up{job="thanos-store-gateway"}`, `up{job="thanos-compactor"}`, and `up{job="kps-thanos-discovery"}`. Each returns value `1`.
- Open Status, then Rule health. Every rule shows `ok`. A rule showing `unknown` right after a reload usually clears within a minute.

### Phase 9: Verify the data path

24. Wait for the first block. Prometheus cuts a 2-hour block only after its in-memory head covers about 3 hours, so allow roughly 3 hours from when the pods started. Watch for the upload:

```bash
kubectl -n $NS logs prometheus-kps-prometheus-0 -c thanos-sidecar | grep "upload new block"
aws s3 ls s3://$BUCKET/ --region $AWS_REGION
```

Each replica uploads its own copy of each block, so expect two directories per time window. Thanos Query removes the duplicates at read time.

25. Confirm the Store Gateway loads the blocks. Port-forward Query, then open the loaded blocks page:

```bash
kubectl -n $NS port-forward svc/thanos-query 10902:10902
```

**Verify:** `http://localhost:10902/loaded` lists the uploaded blocks, and `http://localhost:10902/stores` shows both sidecars and the Store Gateway.

26. Confirm deduplication. In the Thanos Query UI, run:

```
count(up{job="podinfo"})
```

The result must be `3`. If it returns `6`, the `--query.replica-label` value does not match `prometheus_replica`.

### Phase 10: Test cycle

Record the date, the action, and the result of each test. Keep a Grafana panel showing `up{job="podinfo"}` on a 3-hour range during the failover tests.

27. **Health and targets.** All pods `Running`, both nodes `Ready`, and every Prometheus target `UP`.

28. **Sidecar uploads.** Read the counters directly from a sidecar, since Query does not expose them:

```bash
kubectl -n $NS port-forward pod/prometheus-kps-prometheus-0 19102:10902
curl -s localhost:19102/metrics | grep -E "^thanos_shipper_(uploads|upload_failures)_total"
```

Pass: uploads is at least 1 and failures is 0.

29. **Historical reads.** Set Grafana to a time window entirely before the live head. Data returns from S3 through the Store Gateway.

30. **Deduplication.** `count(up{job="podinfo"})` returns 3.

31. **Grafana.** The Thanos datasource passes Save & test, and the imported dashboards (IDs 315 and 1860) show data.

32. **Failover, Prometheus replica:**

```bash
kubectl -n $NS delete pod prometheus-kps-prometheus-0
```

Pass: the graph stays continuous.

33. **Failover, Thanos Query:**

```bash
kubectl -n $NS delete pod -l app=thanos-query --wait=false
```

Pass: Grafana keeps working through the surviving replica.

34. **Failover, Store Gateway:**

```bash
kubectl -n $NS delete pod thanos-store-gateway-0
```

Pass: after restart, the loaded blocks page shows the blocks again and historical queries return data. This shows the data lives in S3, not on the pod's volume.

35. **Alerting.** Scale Query to zero, wait out the 5-minute `for` window, and confirm `ThanosQueryDown` reaches Firing at `http://localhost:9090/alerts`. Then restore:

```bash
kubectl -n $NS scale deploy thanos-query --replicas=0
kubectl -n $NS scale deploy thanos-query --replicas=2
```

36. **Compactor.** Check for repeated errors. Compaction and retention messages are normal. "Overlapping blocks" errors need investigation.

```bash
kubectl -n $NS logs thanos-compactor-0 --tail=50 | grep -i -E "error|overlap|compact|retention"
```

37. **Security.** The bucket must be private and encrypted, no static keys may exist in the cluster, and the Grafana password must not be the default.

```bash
aws s3api get-public-access-block --bucket $BUCKET
aws s3api get-bucket-encryption --bucket $BUCKET
kubectl -n $NS get pod prometheus-kps-prometheus-0 -o jsonpath='{.spec.containers[?(@.name=="thanos-sidecar")].env}'; echo
```

### Phase 11: Commit

38. Check for secrets before you commit:

```bash
git diff --cached | grep -i -E "password|secret|AKIA|aws_secret"
```

No output means it's safe. Then commit and push:

```bash
git add -A
git commit -m "Thanos HA metrics pipeline on EKS: manifests, Helm values, alerts, test log"
git push
```

### Phase 12: Teardown

39. Delete volumes first, then the release, the namespace, and the cluster. Deleting the cluster without deleting PVCs can leave EBS volumes billing:

```bash
kubectl -n $NS delete pvc --all
helm -n $NS uninstall kps
kubectl delete namespace $NS
eksctl delete cluster --name $CLUSTER --region $AWS_REGION
```

40. Confirm nothing billable remains. Each command must return an empty list:

```bash
aws ec2 describe-nat-gateways --region $AWS_REGION --filter Name=state,Values=available --query 'NatGateways[].NatGatewayId'
aws ec2 describe-addresses --region $AWS_REGION --query 'Addresses[?AssociationId==`null`].AllocationId'
aws ec2 describe-volumes --region $AWS_REGION --filters Name=status,Values=available --query 'Volumes[].VolumeId'
```

Keep the S3 bucket. It costs very little, and it's what makes the rebuild test possible.

---

## Verification and test results

| # | Area | What was verified | Result |
|---|---|---|---|
| 1 | Health | All pods Running, nodes Ready | Pass |
| 2 | Scrape targets | All targets `UP` | Pass |
| 3 | Sidecar uploads | `thanos_shipper_uploads_total` > 0, failures = 0 | Pass |
| 4 | Store Gateway | Blocks served from S3 on the loaded blocks page | Pass |
| 5 | Deduplication | `count(up{job="podinfo"})` returns 3, not 6 | Pass |
| 6 | Grafana | Thanos datasource works, dashboards show data | Pass |
| 7 | Failover | Prometheus pod deletion leaves graphs continuous | Pass |
| 8 | Alerting | `ThanosQueryDown` moves to Firing and clears on recovery | Pass |
| 9 | Compactor | Running with no repeated errors | Pass |
| 10 | Security | Private bucket, encryption, no static keys | Pass |

Add screenshots to `docs/` and link them here: the loaded blocks page, the deduplicated query, the failover graph, and the firing alert.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `eksctl create cluster` fails with `AccessDenied` on `ec2:AllocateAddress` | IAM user lacks Elastic IP permission for the NAT gateway | Add the action to the policy, or attach the admin policy in a sandbox account |
| `iam:CreateServiceLinkedRole` error during cluster creation | EKS service-linked role not yet created in the account | Grant `iam:CreateServiceLinkedRole` for the EKS service |
| `kubectl` returns Unauthorized | No access entry for your IAM identity | Create or associate an access entry (phase 1, step 6) |
| Helm error about `control-plane ServiceMonitors` | Chart needs a service account token secret | Let the chart create `prometheus-sa` with `create: true` and the IRSA annotation |
| Thanos pods crash with `mkdir /data/meta-syncer: permission denied` | EBS volume not writable by the non-root user | Add `securityContext.fsGroup: 65534` to the pod spec, then recreate the StatefulSet and its PVC |
| Sidecar logs `no supported bucket was configured` | `objectStorageConfig` not nested under `existingSecret`, so Helm drops it | Fix the nesting, then check `kubectl get prometheus -o jsonpath='{.spec.thanos}'` |
| `thanosService` or `thanosServiceMonitor` has no effect | Keys placed under `prometheusSpec` instead of `prometheus` | Move them to the `prometheus:` level |
| Shipper metrics missing from the Query UI | Sidecar metrics are exported by the sidecar, not by Query | Read them from the sidecar directly, or scrape them via the ServiceMonitor |
| Alert never fires | Its job label has no series, so the expression returns nothing | Add a ServiceMonitor for the target and wrap the expression with `or vector(0)` |
| Rule health shows `unknown` after reload | Rules not evaluated yet | Wait one evaluation cycle (about 30 seconds). If it persists, check the operator and Prometheus logs |
| Overlapping block errors in the Compactor or Store Gateway | Duplicate uploads without the replica label | Confirm `replicaExternalLabelName` matches the Query `--query.replica-label` |
| Empty S3 bucket after a long wait | Head has not reached its cut point, or Prometheus restarted | Wait for the block cut. Restarts reset the head, so avoid restarting during the wait |
| `ls` not found inside the Prometheus container | Hardened images have no shell utilities | Use the API, logs, and S3 to verify instead |

---

## Security notes

- No AWS access keys exist in the cluster or in git. All S3 access uses IRSA.
- The bucket blocks all public access, encrypts objects with SSE-S3, and expires old versions after 7 days.
- The IAM policy is scoped to one bucket.
- The Grafana password lives in a Kubernetes secret, never in a values file.
- Before publishing, confirm the placeholders are in place and no account ID remains in the repo.

---

## Cost

Estimated cost in `ap-south-1` while running: about $1 to $2 around 6 to 7 hours with two Spot worker nodes, one NAT gateway, and about 80 GB of gp3 storage. The EKS control plane is the largest fixed cost. A full build and test session costs a few dollars. Keep the S3 bucket, which costs very little to hold.

---

## Teardown and rebuild

Blocks in S3 are independent of the cluster. To rebuild:

1. Repeat phases 1 to 9 on a new cluster. Do not recreate the bucket.
2. Recreate the secrets in phase 5. The object storage secret comes from `iam/objstore.yml`, and the Grafana password is generated again.
3. Confirm that the Store Gateway loads the old blocks from S3 without re-uploading. This shows that storage and compute are separate.

---

## Possible next steps

- Add Alertmanager receivers (Slack or email) so alerts leave the cluster.
- Move the Helm values into a GitOps tool such as Argo CD.
- Add Thanos Ruler so recording and alerting rules can query the full history.
- Enable `--deduplication.replica-label` on the Compactor to merge replicas in storage.
- Add a second cluster with its own sidecars, to show multi-cluster querying through one Thanos Query.
- Put Grafana behind an ALB with SSO instead of port-forwarding.
