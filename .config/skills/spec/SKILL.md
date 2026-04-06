---
name: spec
description:
  Use `SPEC.md` as the repository source of truth for intended Symphony behavior,
  identify spec/code gaps before implementation, and update the spec whenever a
  behavior or contract change is made.
---

# Spec

## Goals

- Treat `SPEC.md` as the canonical behavior contract for Symphony.
- Distinguish actual implementation gaps from stale specification text.
- Keep the spec and implementation aligned in the same change whenever behavior,
  config semantics, recovery rules, or public interfaces change.

## Workflow

1. Before implementation, open only the relevant `SPEC.md` sections for the subsystem you are changing.
2. Classify every observed difference:
   - spec is current and code is missing behavior
   - code already satisfies behavior and spec is stale
   - code is an intentional extension that needs explicit documentation
3. Implement missing behavior in code when the spec is authoritative.
4. Update `SPEC.md` when:
   - the implementation already matches the intended behavior but the document lags
   - the change intentionally alters contract or semantics
   - a previous `TODO` is no longer true
5. Re-run the most relevant tests and ensure docs, workflow instructions, and skills still match the resulting behavior.

## Required Checks

- Do not change orchestration behavior, config parsing, recovery semantics, tracker semantics, or observability contracts without re-reading the relevant spec section first.
- Do not leave stale `TODO` items in `SPEC.md` when the repository now implements them.
- Do not mark behavior as implemented in the spec unless the code and tests actually support it.

## Typical Areas

- Polling, retries, restart recovery
- Workspace lifecycle and safety
- Tracker operations and dynamic tools
- Optional observability HTTP server
- CLI/config/runtime contract changes
