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
- Cross-machine macOS service bootstrap: `./scripts/setup-macos-service.sh`

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
- Useful workflow extensions:
- `workspace.codex_cwd`: optional spawn cwd for Codex; may be relative to the issue workspace or an explicit absolute path.
- `github.account`: optional GitHub account name exported as `SYMPHONY_GH_REQUIRED_USER` for service-run environments.
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

## Service Deployment

- For deterministic repo-local auto-start on macOS after clone, always use `./scripts/setup-macos-service.sh`.
- Do not hand-roll LaunchAgents, workflow links, or updater jobs when this script is available.
- If required setup information is missing, the setup flow should ask the user for it instead of silently assuming values.
- In particular, `LINEAR_API_KEY` should be prompted for during setup when it is not already present in the environment or `~/.config/symphony/linear_api_key`.
- The LaunchAgent must execute the runner from the cloned repo path so future `git pull` updates on that clone are picked up automatically.
- Workflow activation for the local service should symlink `~/.config/symphony/workflows/symphony.WORKFLOW.md` to `<clone>/elixir/WORKFLOW.md`.
- Install a companion updater LaunchAgent that runs every 3 hours.
- The updater should try a normal `git pull --ff-only` first.
- If the normal pull fails, invoke Codex non-interactively to run the `pull` skill in the cloned repo.
- After a successful update flow, rebuild the escript and restart the Symphony service.
- macOS service runs export `SYMPHONY_HOME=<clone-root>`.
- If a workflow front matter contains `github.account`, macOS service runs should export it as `SYMPHONY_GH_REQUIRED_USER`.
