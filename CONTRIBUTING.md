# Contributing to rrd

Thanks for your interest. `rrd` is a proof-of-concept routing daemon; the bar below
keeps the (currently clean) baseline clean as the project grows.

## Toolchain

The toolchain is pinned in `rust-toolchain.toml` (`rustup` installs it automatically
on first `cargo` invocation). The MSRV is declared as `rust-version` in
`[workspace.package]` in the root `Cargo.toml`.

## The bar (enforced in CI)

Every change must pass, with `--locked` (the committed `Cargo.lock` is authoritative):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --release -p rrd --locked
```

- **Formatting:** `cargo fmt` before committing.
- **Lints:** clippy is run with `-D warnings`; no new warnings.
- **Tests:** the engine units, the `ring-sim` invariant suite, and the daemon tests
  must stay green (0 failures, 0 ignored). New behaviour comes with a test.
- **Determinism:** `ring-core` and `ring-sim` are deterministic by design. Any new
  randomness must flow through the seeded RNG; the engine reads no real clock (time is
  injected). Do not regress this — it is what makes the sim a trustworthy oracle.

## Live netns lab

The live lab (`make demo`/`multihop`/`resilience` and the in-tree `rrd lab`) needs
unprivileged user namespaces (`unshare -rmn`). It is not run by the standard CI
(GitHub-hosted runners), so run it locally when touching the transport/FIB/lab paths.

## Dependencies

Common dependencies are declared once in `[workspace.dependencies]` and referenced
with `foo.workspace = true`. Add shared deps there; keep single-crate deps local.
Keep `ring-core` lean (it is the embeddable kernel).

## Licensing

Contributions are accepted under the project's dual `MIT OR Apache-2.0` license.
