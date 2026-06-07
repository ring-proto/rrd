//! Engine field == exact discrete operator. A centralized solver gives ground truth; a
//! synchronous mesh of engines must relax to the same field (paper §5: the engine computes the
//! exact screened M-matrix solve, distributedly).

use ring_core::{Config, Engine, ForwardingPolicy, Message, NeighborId, Prefix, RouterId};
use std::net::Ipv6Addr;

fn px(n: u16) -> Prefix {
    Prefix {
        addr: Ipv6Addr::new(0xfd00, 0x7269, 0x6e67, n, 0, 0, 0, 0),
        len: 64,
    }
}

/// Centralized Gauss-Seidel solve of (L_g + Q) f = 0, destination `dst` pinned at F0.
/// Mirrors ideal/solver.go's local rule: f_i = (Σ g_ij f_j) / (Σ g_ij + κ).  `n` nodes,
/// `edges` are (i, j, g) undirected.
fn solve(n: usize, edges: &[(usize, usize, f64)], dst: usize, cfg: &Config) -> Vec<f64> {
    let mut adj: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
    for &(i, j, g) in edges {
        adj[i].push((j, g));
        adj[j].push((i, g));
    }
    let mut f = vec![0.0; n];
    f[dst] = cfg.f0;
    for _ in 0..200_000 {
        let mut max_delta = 0.0_f64;
        for i in 0..n {
            if i == dst {
                continue;
            }
            let gsum: f64 = adj[i].iter().map(|&(_, g)| g).sum();
            if gsum <= 0.0 {
                continue;
            }
            let num: f64 = adj[i].iter().map(|&(j, g)| g * f[j]).sum();
            let next = num / (gsum + cfg.kappa);
            max_delta = max_delta.max((next - f[i]).abs());
            f[i] = next; // Gauss-Seidel: use updated values immediately
        }
        if max_delta < 1e-12 {
            break;
        }
    }
    f
}

/// A synchronous in-process mesh of engines for testing. Each round: every engine emits, then
/// every emission is delivered (ingested) to its neighbours. This is the simplest possible
/// driver; the full asynchronous sim (delay/loss/skew) is milestone M1.
struct SyncMesh {
    engines: Vec<Engine>,
    // node index -> list of (neighbor node index, NeighborId that node uses for it)
    wiring: Vec<Vec<(usize, NeighborId)>>,
}

impl SyncMesh {
    fn build(n: usize, edges: &[(usize, usize, f64)], cfg: Config) -> Self {
        let mut engines = Vec::new();
        for i in 0..n {
            engines.push(Engine::new(
                RouterId::from(format!("n{i}").as_str()),
                px((i + 1) as u16),
                cfg,
                ForwardingPolicy::default(),
            ));
        }
        let mut wiring: Vec<Vec<(usize, NeighborId)>> = vec![Vec::new(); n];
        for &(i, j, g) in edges {
            let nid_i = NeighborId(j as u32);
            let nid_j = NeighborId(i as u32);
            engines[i].add_neighbor(nid_i);
            engines[i].set_neighbor_conductance(nid_i, g);
            engines[j].add_neighbor(nid_j);
            engines[j].set_neighbor_conductance(nid_j, g);
            wiring[i].push((j, nid_i));
            wiring[j].push((i, nid_j));
        }
        SyncMesh { engines, wiring }
    }

    fn run(&mut self, rounds: u32, cfg: &Config) {
        for k in 0..rounds {
            let now = cfg.tick * k;
            // 1. snapshot every engine's emission
            let emits: Vec<Message> = self.engines.iter_mut().map(|e| e.step(now).emit).collect();
            // 2. deliver each emission to that node's neighbours
            for src in 0..self.engines.len() {
                let msg = &emits[src];
                for &(nbr, _) in &self.wiring[src] {
                    // find the NeighborId `nbr` uses to refer to `src`
                    let nid = self.wiring[nbr].iter().find(|&&(t, _)| t == src).unwrap().1;
                    self.engines[nbr].ingest(now, nid, msg);
                }
            }
        }
    }
}

#[test]
fn engine_field_matches_centralized_solver_on_a_line() {
    let mut cfg = Config::default();
    // Exact static M-matrix oracle: raw conductances, no staleness sawtooth.
    cfg.no_link_age = true;
    // 5-node line: 0-1-2-3-4, unit conductance. Destination = node 0.
    let edges = [(0, 1, 1.0), (1, 2, 1.0), (2, 3, 1.0), (3, 4, 1.0)];
    let n = 5;
    let dst = 0;

    let truth = solve(n, &edges, dst, &cfg);

    let mut mesh = SyncMesh::build(n, &edges, cfg);
    mesh.run(2000, &cfg); // ample for degree-≤2 contraction to converge

    let dst_id = RouterId::from(format!("n{dst}").as_str());
    for i in 0..n {
        if i == dst {
            continue;
        }
        let f = mesh.engines[i].peek_pot(&dst_id).unwrap();
        assert!(
            (f - truth[i]).abs() < 1e-3,
            "node {i}: engine f={f} vs solver f*={}",
            truth[i]
        );
    }
}

#[test]
fn engine_field_matches_centralized_solver_on_a_mesh() {
    let mut cfg = Config::default();
    // Exact static M-matrix oracle: raw conductances, no staleness sawtooth.
    cfg.no_link_age = true;
    // A small irregular mesh with a mixed-conductance edge.
    let edges = [
        (0, 1, 1.0),
        (1, 2, 1.0),
        (2, 3, 1.0),
        (0, 3, 1.0),
        (1, 3, 5.0), // fat diagonal
    ];
    let n = 4;
    let dst = 2;

    let truth = solve(n, &edges, dst, &cfg);

    let mut mesh = SyncMesh::build(n, &edges, cfg);
    mesh.run(3000, &cfg);

    let dst_id = RouterId::from(format!("n{dst}").as_str());
    for i in 0..n {
        if i == dst {
            continue;
        }
        let f = mesh.engines[i].peek_pot(&dst_id).unwrap();
        assert!(
            (f - truth[i]).abs() < 1e-3,
            "node {i}: engine f={f} vs solver f*={}",
            truth[i]
        );
    }
}
