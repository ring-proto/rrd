# rrd CNI + single-node RKE2 bring-up

Status: M0 CNI shim exists as workspace crate `rrd-cni`.

## What exists now

`rrd-cni` is a short-lived CNI plugin intended for chained mode.

Implemented CNI commands:

- `VERSION`: prints supported CNI versions.
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
printf 'cni: flannel\n' | sudo tee /etc/rancher/rke2/config.yaml
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
cni: flannel
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
```

The active CNI conflist is:

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
    }
  ]
}
```

DNS smoke from a temporary BusyBox pod resolved fully-qualified cluster service names, e.g. `kubernetes.default.svc.cluster.local -> 10.43.0.1`. BusyBox's short-name `nslookup kubernetes.default.svc` returned NXDOMAIN despite the pod search domains being present; use FQDNs in smoke tests to avoid BusyBox resolver ambiguity.

## First chained CNI config sketch

Once `rrd-agent` exists and listens on `/run/rrd/agent.sock`, install `rrd-cni` as a chained plugin after the primary plugin. The exact file should be derived from the live RKE2 CNI config under `/etc/cni/net.d`.

Conceptual shape:

```json
{
  "cniVersion": "1.0.0",
  "name": "rrd-chain",
  "plugins": [
    {
      "type": "<existing-rke2-primary-plugin>"
    },
    {
      "type": "rrd-cni",
      "socket": "/run/rrd/agent.sock",
      "advertise": "pod"
    }
  ]
}
```

Do not blindly overwrite RKE2's generated config. First inspect `/etc/cni/net.d`, then create a reversible backup and patch only after we know the exact active config shape.

## Next implementation milestone

M1 should add the long-running consumer:

```text
rrd-agent
  - binds /run/rrd/agent.sock
  - accepts newline-delimited JSON lifecycle events from rrd-cni
  - stores local pod records by container_id
  - exposes inspection: local pods, advertised prefixes, route state
```

Until M1 lands, `rrd-cni` is intentionally tested against a fake Unix socket agent.
