# Multi-cloud Conflux Image & Small-network Test Plan 🚀

## Objective

Test the image-build and small multi-cloud Conflux deployment workflow safely and cheaply:
- Build server images with Docker + Conflux pre-pulled on **Alibaba Cloud** and **AWS**.
- Launch a small number of instances (1 per cloud) using those images, start Conflux node(s), and run a basic test across the multi-cloud network.
- Validate behavior and collect logs.

This plan is intentionally conservative on resource usage and cost.

---

## Assumptions & Safety Notes ⚠️

- You have cloud accounts for **AWS** and **Alibaba Cloud** with credentials and permission to create instances, security groups, and images.
- You have an SSH key pair for each cloud (or shared) and the private key is available locally (path in config).  Ensure correct permissions: `chmod 600 ~/.ssh/mykey.pem`.
- This plan uses very small instance counts (1 per provider) and small instance types. Expect a small charge for temporary image-builder instance and the test instance(s).
- Always run a controlled cleanup after tests: `conflux-deployer cleanup -c config_test.json --force --delete-images`.

---

## Top-level Files

- `config_test.json` — A minimal deployment configuration you will edit with credentials, SSH key info, and two small regions (AWS + Alibaba).
- `state/` — ConfluxDeployer saves state here (or according to `state_file_path` in config). Check a state file to find image IDs and instance IDs.

---

## Example minimal `config_test.json`

```json
{
  "deployment_id": "conflux-mini-test-1",
  "instance_name_prefix": "conflux-mini",
  "credentials": {
    "aws": {
      "access_key_id": "YOUR_AWS_KEY",
      "secret_access_key": "YOUR_AWS_SECRET",
      "key_pair_name": "conflux-keypair",
      "key_pair_path": "~/.ssh/conflux-keypair.pem"
    },
    "alibaba": {
      "access_key_id": "YOUR_ALI_KEY",
      "secret_access_key": "YOUR_ALI_SECRET",
      "key_pair_name": "conflux-keypair",
      "key_pair_path": "~/.ssh/conflux-keypair.pem"
    }
  },
  "regions": [
    {
      "provider": "aws",
      "region_id": "us-west-2",
      "instance_count": 1,
      "nodes_per_instance": 1,
      "instance_type": "t3.medium"
    },
    {
      "provider": "alibaba",
      "region_id": "cn-hangzhou",
      "instance_count": 1,
      "nodes_per_instance": 1,
      "instance_type": "ecs.t5-lc1m2.small"
    }
  ],
  "image": {
    "image_name_prefix": "conflux-node-image",
    "conflux_docker_image": "confluxchain/conflux-rust:latest",
    "ubuntu_version": "22.04",
    "additional_packages": ["curl", "jq", "ca-certificates"]
  },
  "cleanup": {
    "auto_terminate": true,
    "delete_images": false
  }
}
```

Notes:
- `image.additional_packages` will be included into the user-data `apt-get install` line.
- `ssh_private_key_path` is stored in config loader `ssh_private_key_path` (use `key_pair_path`).

---

## Preparations (Local)

1. Ensure the repo venv is active and tests/linters pass locally (optional):

```bash
source .venv/bin/activate
.venv/bin/pytest -q
```

2. Prepare SSH key pair(s):

- AWS (CLI):

```bash
aws ec2 create-key-pair --key-name conflux-keypair --query 'KeyMaterial' --output text > ~/.ssh/conflux-keypair.pem
chmod 600 ~/.ssh/conflux-keypair.pem
```

- Alibaba: import key via console or using `aliyun` CLI (or reuse the same key if already installed). Example using public key content:

```bash
aliyun ecs ImportKeyPair --RegionId cn-hangzhou --KeyPairName conflux-keypair --PublicKeyBody "$(cat ~/.ssh/conflux-keypair.pub)"
```

3. Update `config_test.json` with your access keys and key pair names/paths.

4. (Optional) Set environment variables instead of writing secrets to the config file:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export ALIBABA_ACCESS_KEY_ID=...
export ALIBABA_SECRET_ACCESS_KEY=...
```

---

## Step A — Build Images Only (first test)

Goal: Build OS instances with Docker + Conflux image pre-pulled, in both clouds (no long-running instances left behind).

1. Ensure `config_test.json` is configured and saved.

2. Create images via ConfluxDeployer CLI:

```bash
# create images (will write state under ./state/<deployment_id>.json)
python -m conflux_deployer.cli images -c config_test.json create
```

- Use `-f` to force recreation: `... images -c config_test.json create -f`.

3. Monitor logs for progress — expected phases:
  - security group creation (temporary)
  - launch temporary instance (t3.medium / ecs...)
  - user-data progress (look for "Conflux node image setup complete")
  - create image on cloud and save image id into state

4. Verify images exist:

- Using CLI:

```bash
python -m conflux_deployer.cli images -c config_test.json find
```

- AWS CLI quick check:

```bash
aws ec2 describe-images --owners self --filters "Name=name,Values=conflux-node-image*" --region us-west-2
```

- Alibaba CLI quick check (example):

```bash
aliyun ecs DescribeImages --RegionId cn-hangzhou --ImageName "conflux-node-image*" --ImageOwnerAlias self
```

5. If image creation fails, inspect the temporary instance logs:

```bash
# Example: SSH into the image builder (public IP from state) and view user-data logs
ssh -i ~/.ssh/conflux-keypair.pem ubuntu@<builder-ip> 'sudo tail -n 200 /var/log/user-data.log'
# Also check for markers
ssh -i ~/.ssh/conflux-keypair.pem ubuntu@<builder-ip> 'ls -l /var/lib/conflux-setup-*'
```

Troubleshooting tips:
- If SSH cannot connect, ensure the temporary security group allows TCP/22 from your IP.
- If Docker pull fails, inspect `user-data` logs for network/DNS errors and increase retry timings if needed.

---

## Step B — Launch small nodes using created images

Goal: Launch one instance in AWS and one in Alibaba using the images created and ensure Conflux node can be started from `/usr/local/bin/start_conflux.sh`.

1. Option A (recommended — use state images):
   - If images were created by Step A, the deployer state already contains the image IDs. Run the `deploy` command which will use those images:

```bash
# Deploy the configured instances (1 per region as configured)
python -m conflux_deployer.cli deploy -c config_test.json
```

2. Option B (explicit image IDs):
   - Edit `config_test.json` to include `image.existing_images` mapping with provider/region → image_id (if you want to pin an explicit image).

3. Check deployment status and instance IPs:

```bash
python -m conflux_deployer.cli status -c config_test.json
# Or check the state file under `state/` folder
cat state/conflux-mini-test-1.json
```

4. Validate availability on the instance(s):

```bash
# Replace <ip> with the instance public IP
ssh -i ~/.ssh/conflux-keypair.pem ubuntu@<ip> 'sudo test -f /var/lib/conflux-setup-complete && echo OK || echo NOT_READY'
ssh -i ~/.ssh/conflux-keypair.pem ubuntu@<ip> 'sudo docker image inspect confluxchain/conflux-rust:latest >/dev/null 2>&1 && echo IMAGE_OK || echo IMAGE_MISSING'
ssh -i ~/.ssh/conflux-keypair.pem ubuntu@<ip> 'sudo /usr/local/bin/start_conflux.sh 0 /data/conflux/config && sleep 5 && sudo docker ps -a | grep conflux_node'
```

5. If node fails to start, collect logs:

```bash
ssh -i ~/.ssh/conflux-keypair.pem ubuntu@<ip> 'sudo journalctl -u docker --no-pager -n 200'
ssh -i ~/.ssh/conflux-keypair.pem ubuntu@<ip> 'sudo docker logs conflux_node_0'
```

---

## Step C — Connect nodes and run a small multi-cloud test

Goal: Start Conflux nodes on both clouds, connect them, and perform a short latency or stress test.

1. With instances running and node processes started, use the built-in test commands:

```bash
# Run a simple latency test (100 samples)
python -m conflux_deployer.cli test latency -c config_test.json --samples 100

# Or run a short stress test for 120 seconds
python -m conflux_deployer.cli test stress -c config_test.json --duration 120
```

2. Alternatively use the remote_simulate_v2 script to run the full scenario (create -> test -> cleanup) in one shot (be cautious; this will create instances if not present):

```bash
# Example: run using remote_simulate_v2 (creates instances, runs test, then cleans up unless --no-cleanup)
python remote_simulate_v2.py run --instance-count 2 --instance-type t3.medium
```

3. Validate test results and collect logs:

- Check CLI output metrics
- Use the deployer `collect_metrics()` or check node RPC endpoints
- Collect logs using the async SSH helper already integrated, or:

```bash
# Collect logs (manual)
scp -i ~/.ssh/conflux-keypair.pem ubuntu@<ip>:/data/conflux/logs/* ./logs/<provider>-<ip>/
```

---

## Cleanup (Always run after test) ✅

**Important:** Delete instances and (optionally) images to avoid ongoing costs.

```bash
# Cleanup instances only
python -m conflux_deployer.cli cleanup -c config_test.json

# Cleanup and also delete images
python -m conflux_deployer.cli cleanup -c config_test.json --force --delete-images

# Alternatively remove images alone
python -m conflux_deployer.cli images -c config_test.json delete
```

---

## Validation Checklist (what to confirm)

- [ ] Image(s) created and visible in cloud consoles and in `state/*.json`.
- [ ] Temporary image-builder instances terminated and security groups cleaned up.
- [ ] Deployed instance(s) are running and reachable over SSH.
- [ ] `/var/lib/conflux-setup-complete` exists on each instance.
- [ ] Docker image `confluxchain/conflux-rust:latest` present on instances.
- [ ] `start_conflux.sh` launches a Docker container successfully.
- [ ] Tests complete and produce metrics/logs.
- [ ] Resources fully cleaned up after testing.

---

## Troubleshooting tips

- If `images create` fails with timeout, inspect temporary builder logs: `/var/log/user-data.log` and `/var/lib/conflux-setup-failed` marker.
- If SSH fails: check security groups and key pair config. Temporary builder uses security group created by image manager — allow inbound TCP/22 from your IP.
- If Docker pull fails on builder: check network egress access (NAT / Internet access) and repository access.

---

## Optional follow-ups

- Add a test-run CI job that performs the image build in a sandboxed account with strict budgets/quotas.
- Add unit tests mocking `RemoteExecutor` to verify `_wait_for_image_builder_ready` behavior.
- Make timeouts configurable from `config_test.json` (ImageManager currently uses defaults; we can expose these soon).

---

If you like, I can:
- Create a ready-to-run `config_test.json` file (with placeholders) in the repo,
- Add a small wrapper script to run Steps A→C and produce a concise report,
- Or implement the optional follow-ups above.

Which of these would you like next? 👇
