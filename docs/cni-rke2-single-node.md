# rrd CNI + single-node RKE2 bring-up

Status: M1 live chained CNI observer is installed on the local RKE2/Flannel node.

- `rrd-cni` is installed in `/opt/cni/bin/rrd-cni`.
- `rrd-agent` is installed in `/usr/local/bin/rrd-agent` and runs as `rrd-agent.service`.
- `rrdctl` is installed in `/usr/local/bin/rrdctl`.
- Flannel's active conflist appends `rrd-cni` after `flannel -> portmap -> bandwidth`.

## What exists now

`rrd-cni` is a short-lived CNI plugin intended for chained mode.

Implemented CNI commands:

- `VERSION`: prints supported CNI versions, including RKE2 Flannel's `0.3.1` conflist version.
- `ADD`: parses CNI stdin/env, extracts `prevResult.ips[*].address`, emits a `pod_added` JSON event to a Unix socket, and returns `prevResult` unchanged.
- `DEL`: emits `pod_deleted` when possible and succeeds idempotently if the agent socket is absent.
- `CHECK`: emits `pod_checked` and writes no stdout on success.

Default agent socket:

```text
/run/rrd/agent.sock
```

CNI stdout discipline:

- normal `ADD`/`VERSION` write protocol JSON to stdout;
- successful `DEL`/`CHECK` write nothing;
- no debug logging is emitted to stdout.

## Build

```bash
cargo build -p rrd-cni
```

The binary is:

```text
target/debug/rrd-cni
```

For a node install, copy the release binary into the active CNI bin dir, commonly:

```bash
sudo install -m 0755 target/release/rrd-cni /opt/cni/bin/rrd-cni
```

RKE2 may also expose packaged tools under `/var/lib/rancher/rke2/bin`; check the live node before installing.

## Current host readiness snapshot

Observed on this machine before any RKE2 mutation:

```text
user: eve
uid: 1000
arch: aarch64
systemd: available
containerd: /usr/bin/containerd
rke2: not installed in PATH
kubectl: not installed in PATH
sudo -n true: yes
swap entries: 1
port 6443 listeners: 0
root filesystem: 916G total, 470G available
```

Swap being enabled is worth remembering. If kubelet/RKE2 rejects swap on this host, either disable swap or set an explicit kubelet config after verifying the target RKE2 version behavior.

## Safe single-node RKE2 Flannel install

Do this only after deciding this host should be mutated into a local cluster.

Use Flannel rather than the RKE2 default Canal for the RRD lab host. Flannel is enough for pod overlay networking and avoids Calico/Canal's extra policy machinery.

```bash
sudo mkdir -p /etc/rancher/rke2
sudo tee /etc/rancher/rke2/config.yaml <<'EOF'
tls-san:
- spark.local
cni: flannel
cluster-domain: spark.local
EOF
curl -sfL https://get.rke2.io | sudo sh -
sudo systemctl enable --now rke2-server
sudo systemctl status rke2-server --no-pager
```

Kubeconfig and bundled kubectl path are typically:

```bash
sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml get nodes -o wide
```

For ergonomic local access:

```bash
mkdir -p ~/.kube
sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/config
sudo chown "$USER:$USER" ~/.kube/config
export PATH="$PATH:/var/lib/rancher/rke2/bin"
kubectl get nodes -o wide
```

## Recommended RKE2 settings for first RRD pass

Use a minimal single-node RKE2 install with Flannel:

- CNI: Flannel (`cni: flannel`)
- default cluster/service CIDRs (`10.42.0.0/16` pods, `10.43.0.0/16` services on this install)
- single server node

Do not make RRD the primary CNI yet. Start as a chained observer/control-plane hook beside RKE2's working Flannel CNI.

RKE2 docs note that bundled CNIs include Canal, Cilium, Calico, and Flannel, with Canal as the default. For RRD, Flannel is the better first target: less invasive, simpler dataplane, and sufficient for lifecycle/routing experiments while `rrd-cni` observes pod lifecycle and feeds `rrd-agent`.

## Verified local RKE2 state

This host was brought up with:

```yaml
tls-san:
- spark.local
cni: flannel
cluster-domain: spark.local
```

Verified state:

```text
RKE2: v1.35.5+rke2r2
node: spark Ready, roles control-plane,etcd
runtime: containerd://2.2.3-k3s1
Flannel DaemonSet: kube-system/kube-flannel-ds, 1/1 ready
CNI config: /etc/cni/net.d/10-flannel.conflist
CNI binary: /opt/cni/bin/flannel
pod CIDR: 10.42.0.0/16
service CIDR: 10.43.0.0/16
cluster domain: spark.local
cluster DNS: 10.43.0.10
```

The active CNI conflist is now:

```json
{
  "name": "cbr0",
  "cniVersion": "0.3.1",
  "plugins": [
    {
      "type": "flannel",
      "delegate": {
        "hairpinMode": true,
        "isDefaultGateway": true
      }
    },
    {
      "type": "portmap",
      "capabilities": {
        "portMappings": true
      }
    },
    {
      "type":"bandwidth",
      "capabilities": {
        "bandwidth": true
      }
    },
    {
      "type": "rrd-cni",
      "socket": "/run/rrd/agent.sock",
      "advertise": "pod"
    }
  ]
}
```

A base rollback copy exists at:

```text
/etc/cni/net.d/10-flannel.conflist.base-before-rrd
```

DNS smoke from temporary BusyBox pods resolved:

- `kubernetes.default.svc.spark.local -> 10.43.0.1`
- `rke2-coredns-rke2-coredns.kube-system.svc.spark.local -> 10.43.0.10`

The pod resolver search path is now:

```text
search default.svc.spark.local svc.spark.local spark.local
nameserver 10.43.0.10
options ndots:5
```

BusyBox `nslookup kubernetes.default.svc` still returned NXDOMAIN because BusyBox `nslookup` does not reliably apply the pod search suffixes. Real libc-style resolution did work: BusyBox `wget https://kubernetes.default.svc/version` and `wget https://kubernetes.default.svc.spark.local/version` both reached the API server and returned HTTP 401, which proves DNS/connectivity worked.

Note: `.local` is mDNS-reserved outside Kubernetes. That is intentional here because the host is reached as `spark.local`; keep in mind that host-level `.local` lookups and in-cluster CoreDNS `.spark.local` are separate resolver contexts.

## Live M1 smoke test

A temporary BusyBox pod exercised the real CNI chain:

```bash
kubectl run rrd-cni-smoke --image=busybox:1.36 --restart=Never --command -- sleep 120
kubectl wait --for=condition=Ready pod/rrd-cni-smoke --timeout=120s
rrdctl pods
kubectl delete pod rrd-cni-smoke
rrdctl status
```

Observed `pod_added` state:

```json
{
  "container_id": "9723d277d0a5b3e5e0ef9bb4f1432238baa156963ed5c0b5d2ab3e134b8c99f4",
  "netns": "/var/run/netns/cni-8ce660ab-8ab5-e5d0-75a9-eb915e73806b",
  "ifname": "eth0",
  "network": "cbr0",
  "ips": ["10.42.0.27/24"]
}
```

After deletion, `rrdctl status` showed:

```json
{
  "add_events": 1,
  "delete_events": 1,
  "events_seen": 2,
  "invalid_events": 0,
  "pod_count": 0
}
```

The node remained Ready and `kube-system/kube-flannel-ds` remained 1/1 ready after chaining.

## Next implementation milestone

M2 should connect lifecycle observations to RRD routing intent:

```text
rrd-agent
  - maps pod_added/pod_deleted into advertised local prefixes
  - supports advertise=pod and advertise=nodePodCIDR modes
  - exposes advertised prefixes and eventually desired/installed routes via rrdctl
  - prepares the handoff to the RING daemon/dataplane route installer
```
