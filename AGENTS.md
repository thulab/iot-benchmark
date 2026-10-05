# Repository guidance

IoT Benchmark is a Java 17 / Maven multi-module database benchmark. `core/` owns workloads, configuration, measurements and `IDatabase`; database modules implement adapters; `configuration/` supplies shared distribution files.

## Working approach

- Read only task-relevant files: `README.md`/`README-cn.md` for build/run, the affected module for local behavior, and `docs/DeveloperGuide.md` for architecture or extension work. Small documentation edits do not require a full-repository read.
- Before editing, inspect `git status --short`, the current diff, and the exact write set. Preserve unrelated changes and nested repositories. Complete authorized work through focused verification and diff review; resolve routine choices locally. Ask only when a choice changes scope, interface, semantics, or an external/destructive action.
- User instructions take precedence over repository workflow and skill defaults, subject to higher-priority instructions and execution permissions. If a local rule blocks work, identify the exact file and rule, explain the conflict and continue independent work.
- Use a short plan for multi-step or ambiguous work. Parallelize independent reads/checks and delegate only bounded work when coordination saves time; avoid duplicate reviews and tests.
- Treat old plans under ignored `docs/superpowers/` as historical context, not active instructions. Verify any reused claim against current source.

## Verification

Run from the repository root with JDK 17. Choose the narrowest check for the changed behavior:

| Change | Starting check |
| --- | --- |
| Documentation or agent instructions | Check links, facts and `git diff --check`; no Java build needed |
| Core behavior | `mvn -B test -pl core` |
| One test while iterating | `mvn -B test -pl core -Dtest=OperationControllerTest` (replace the class as needed) |
| Adapter / packaging | `mvn -B package -pl iotdb-2.0 -am -DskipTests` (replace the module); add tests relevant to behavior |
| Java formatting | `mvn -B spotless:check -pl core` (replace or expand the module list) |

Spotless runs during Maven `validate`; do not repeat an equivalent successful check. Format only affected modules and inspect the diff. Use `clean` for stale output or clean-build verification, not every iteration. Expand tests for shared contracts, failures, or unresolved risk; stop when selected checks pass. Packaging with skipped tests is not test success.

For core/configuration/adapter development, use the repository skill [iot-benchmark-development](.agents/skills/iot-benchmark-development/SKILL.md) for the relevant pitfalls and extension workflow. It is unnecessary for ordinary prose edits.

Report the outcome, checks actually run and any remaining limitation in concise language matching the user. Performance claims require measured evidence.
