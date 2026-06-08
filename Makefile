# rrd — convenience targets.

RTPROTO_SRC := etc/rt_protos.d/ring.conf
RTPROTO_DST := /etc/iproute2/rt_protos.d/ring.conf

.PHONY: build test demo multihop resilience install-rtproto uninstall-rtproto

build:                ## release build of the daemon
	cargo build --release -p rrd

test:                 ## full workspace test suite
	cargo test

demo:                 ## rootless live netns routing demo — 2-hop milestone (no sudo)
	bash harness/demo.sh

multihop:             ## rootless live MULTI-hop demo — real packets across a 4-hop line at the default reach
	bash harness/multihop.sh

resilience:           ## rootless live resilience tests (loss tolerance, partition/merge, flap storm)
	bash harness/resilience.sh

# ── proto-name mapping (DISPLAY ONLY) ────────────────────────────────────────
# The kernel always stores RING's route protocol numerically (155); the daemon
# installs and the demo asserts on `proto 155`, so nothing here is required for
# correctness. This mapping only makes `ip -6 route show` print the friendly
# `proto ring` instead of `proto 155`. It is a one-line, host-global, root-owned
# file under /etc — install/remove it explicitly (it is NOT touched by the lab,
# which is rootless and cannot write /etc).

install-rtproto:      ## install the 155->ring name mapping so `ip route` shows `proto ring`
	sudo install -D -m 0644 $(RTPROTO_SRC) $(RTPROTO_DST)
	@echo "installed $(RTPROTO_DST); 'ip -6 route show' now displays 'proto ring'."

uninstall-rtproto:    ## remove the name mapping
	sudo rm -f $(RTPROTO_DST)
	@echo "removed $(RTPROTO_DST)."
