#!/usr/bin/env python3
"""Mutation testing for the simulator: does it catch deliberately injected bugs?

Each mutant is a one-line change that breaks a Raft or KV invariant. The script copies
the crate to a temporary directory, applies one mutant at a time, runs the unit tests,
then builds `raft-sim` and runs it over a range of seeds under two fault profiles. A
mutant is "killed" by a column if any test (or any seed) fails.

    python3 scripts/mutation_test.py               # 2000 seeds per profile
    python3 scripts/mutation_test.py --seeds 500

The working tree is never modified.
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (name, file, original text, mutated text)
MUTANTS = [
    (
        "votes ignore the log up-to-date check (5.4.1)",
        "src/raft/node.rs",
        "let up_to_date = self.log.is_up_to_date(last_log_index, last_log_term);",
        "let up_to_date = true;",
    ),
    (
        "a node may vote twice in one term",
        "src/raft/node.rs",
        "let granted = can_vote && up_to_date;",
        "let granted = up_to_date;",
    ),
    (
        "leader commits old-term entries by counting replicas (Figure 8)",
        "src/raft/node.rs",
        "if candidate > self.commit_index && self.log.term(candidate) == Some(self.term) {",
        "if candidate > self.commit_index {",
    ),
    (
        "follower keeps conflicting entries instead of truncating",
        "src/raft/log.rs",
        "if self.term(entry.index) == Some(entry.term) {",
        "if true {",
    ),
    (
        "follower commit not bounded by last new entry",
        "src/raft/node.rs",
        "let new_commit = leader_commit.min(last_new);",
        "let new_commit = leader_commit.min(self.log.last_index());",
    ),
    (
        "ReadIndex answered without a heartbeat round",
        "src/raft/node.rs",
        "if self.quorum() == 1 {\n            self.ready_reads.push(",
        "if true {\n            self.ready_reads.push(",
    ),
    (
        "ReadIndex confirmed by one follower ack instead of a quorum",
        "src/raft/node.rs",
        "if acks < quorum {",
        "if acks < 2 {",
    ),
    (
        "term and vote are not persisted",
        "src/storage.rs",
        "                state.hard_state = *hs;\n            }\n            if let Some(w) = log_write {",
        "                let _ = hs;\n            }\n            if let Some(w) = log_write {",
    ),
    (
        "client request deduplication disabled",
        "src/kv/mod.rs",
        "if req.seq == session.last_seq {",
        "if false {",
    ),
]

PROFILES = [
    ("default", []),
    ("harsh, no pre-vote", ["--crash-rate", "4", "--partition-rate", "4", "--no-pre-vote"]),
]


def run(cmd, cwd, env=None):
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=2000)
    args = ap.parse_args()

    work = tempfile.mkdtemp(prefix="raft-kv-mutants-")
    env = dict(os.environ, CARGO_TARGET_DIR=os.path.join(work, "target"))
    try:
        crate = os.path.join(work, "crate")
        shutil.copytree(
            ROOT, crate, ignore=shutil.ignore_patterns("target", "data", ".git")
        )
        headers = ["unit tests"] + [f"sim: {p[0]}" for p in PROFILES]
        print(f"{'mutant':<62} " + "  ".join(f"{h:>20}" for h in headers))
        print("-" * (62 + 22 * len(headers)))
        for name, path, old, new in MUTANTS:
            file = os.path.join(crate, path)
            original = open(file).read()
            if old not in original:
                print(f"{name:<62} MUTATION SITE NOT FOUND", file=sys.stderr)
                sys.exit(2)
            open(file, "w").write(original.replace(old, new))
            build = run(["cargo", "build", "--release", "--bin", "raft-sim"], crate, env)
            if build.returncode != 0:
                print(build.stderr, file=sys.stderr)
                sys.exit(2)
            unit = run(["cargo", "test", "--lib", "--quiet"], crate, env)
            cells = ["killed" if unit.returncode != 0 else "survived"]
            for _, flags in PROFILES:
                out = run(
                    [os.path.join(env["CARGO_TARGET_DIR"], "release", "raft-sim"),
                     "--seeds", str(args.seeds)] + flags,
                    crate,
                    env,
                ).stdout
                failed = sum(1 for line in out.splitlines() if line.startswith("FAIL "))
                cells.append(
                    f"killed ({failed}/{args.seeds})" if failed else "survived"
                )
            print(f"{name:<62} " + "  ".join(f"{c:>20}" for c in cells), flush=True)
            open(file, "w").write(original)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
