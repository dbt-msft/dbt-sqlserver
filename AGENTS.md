# AGENTS.md

Conventions for contributors and AI agents. Setup, CI and releasing are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Setup

- `make dev` installs dependencies and the pre-commit hooks; pre-commit gates every commit.
- `make server` starts SQL Server on port 1433; `cp test.env.sample test.env` holds the credentials.
- `make unit` and `make functional` run the suites. Functional tests are the proof that a fix works.

## Workflow

1. **Scope.** Read the issue and find the macro or Python path involved (`dbt/include/sqlserver/macros/`, `dbt/adapters/sqlserver/`). Check whether dbt-core or dbt-adapters already handles it upstream. Check whether this repository already has a recent implementation or approach to follow.
2. **Reproduce.** Write a failing functional test against the live server.
3. **Plan.** When there's more than one reasonable fix, or it changes behavior users rely on, post the options and a recommendation in the issue before writing code. Without built-in planning, keep options and todos in `.plan/` (gitignored) and delete it when done.
4. **Decide on evidence.** Base each decision on code, docs, versions or a live query. Don't guess, and don't iterate by blind trial and error.
5. **Fix.** Make the smallest change that turns the test green, then run `make unit` and the affected functional tests (`uv run pytest tests/functional/<path>`) before the full `make functional`.
6. **PR.** Fill in the [template](.github/pull_request_template.md). For user-facing changes, add a `changes/<issue>.<type>.md` fragment (`+<slug>.<type>.md` without an issue), where `<type>` is `behavior`, `feature` or `bugfix`. Don't edit `CHANGELOG.md`.

## Text

Brief by default: code, comments, docs, changelog entries, commits, issues and PR descriptions. Cut any sentence that repeats the one before it. When the point is a structure (branch topology, a state machine, a build path), draw a diagram instead of describing it:

```
issue #838
   ├── fix/838-slug ───────────→ master          "Closes #838"
   └── backport/1.11-838-slug ─→ release/v1.11   "Fixes #838 on the 1.11 line"
```

When the structure isn't the point, leave the diagram out. Commits follow Conventional Commits (`fix(scope): ...`).

## Code

Write the simplest change that solves the problem. Avoid workarounds, speculative abstractions and options no caller uses.

## Comments

Only write what the code beside the comment cannot show. Don't narrate history or restate the code. Do keep the gotchas: why a call runs once and not twice, which direction a check fails, why a value is pinned or escaped. If a comment asserts a deliberate choice, update it when the choice changes.

## Tests

- Verify SQL behavior by running it against a live server (`tests/functional/`). Assert on emitted SQL only for a property the server cannot show, as `test_full_refresh_marker_sql.py` and `test_table_build_sql.py` do. Never assert on SQL instead of executing it.
- Test behavior and integration; the flow already exercises parameters and configs. Don't write trivial assertions (text in text, param in params), redundant checks, or tests that cannot fail.
- Build one dbt project per class. When cases are phases of one object's life, walk them in sequence in a single test instead of a class each.
- Keep throwaway local checks in `tests/local/`, which is gitignored.
- Mark a test that only matters until a release or deployment with `@pytest.mark.temporary(reason="remove after <version>")`.
