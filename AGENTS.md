# Symphony

This repository contains the Symphony orchestration service and its language-agnostic specification.

## Canonical Paths

- Repository-wide instructions live in this file.
- Repository-local Claude instructions must be a symlink: `.claude/CLAUDE.md -> ../AGENTS.md`
- Repository-local skills live under `.config/skills/`
- `.codex/skills`, `.claude/skills`, and `.cursor/skills` must stay symlinked to `../.config/skills`

## Environment

- Elixir: `1.19.x` on OTP 28 via `mise`
- Install deps: `cd elixir && mise trust && mise exec -- mix setup`
- Main quality gate: `cd elixir && make all`

## Required Skills

- Use the `spec` skill before implementation work that changes behavior, config semantics, orchestration, or public interfaces.
- Use the `pull` skill before merge-based branch sync.
- Use the `push` skill before publishing a branch or creating/updating a PR.
- Use the `land` skill for merge completion once a ticket reaches `Merging`.

## GitHub Account Policy

- This repository's GitHub operations must run as `TT-AIXion`.
- Before any `gh` command or GitHub-backed `git pull`, `git push`, `git fetch`, or `git ls-remote`, check the active `gh` account.
- If the active account is not `TT-AIXion`, switch to `TT-AIXion`, run the command, then restore the original active account.
- Restore the original account even when the GitHub operation fails.
- Prefer the repository helper: `./scripts/with-gh-account.sh ...`

## Codebase-Specific Conventions

- Runtime config is loaded from `WORKFLOW.md` front matter via `SymphonyElixir.Workflow` and `SymphonyElixir.Config`.
- Keep the implementation aligned with `SPEC.md` where practical.
- The implementation may be a superset of the spec.
- The implementation must not conflict with the spec.
- If implementation changes meaningfully alter intended behavior, update `SPEC.md` in the same change when practical.
- Prefer adding config access through `SymphonyElixir.Config` instead of ad-hoc env reads.
- Workspace safety is critical:
- Never run Codex turn cwd in the source repo.
- Workspaces must stay under the configured workspace root.
- Orchestrator behavior is stateful and concurrency-sensitive; preserve retry, reconciliation, cleanup, and restart-recovery semantics.
- Follow `elixir/docs/logging.md` for logging conventions and required issue/session context fields.

## Required Rules

- Public functions (`def`) in `elixir/lib/` must have an adjacent `@spec`.
- `defp` specs are optional.
- `@impl` callback implementations are exempt from the local `@spec` requirement.
- Keep changes narrowly scoped; avoid unrelated refactors.
- Follow existing module/style patterns in `elixir/lib/symphony_elixir/*`.

## Tests And Validation

- Run targeted tests while iterating.
- Run `cd elixir && make all` before handoff.
- Run `cd elixir && make e2e` only when intentionally validating real external Linear/Codex orchestration.

## Docs Update Policy

- If behavior or config changes, update docs in the same PR:
- `README.md` for project concept and goals
- `elixir/README.md` for implementation and run instructions
- `SPEC.md` for specification changes
- `elixir/WORKFLOW.md` for workflow/prompt/config contract changes
