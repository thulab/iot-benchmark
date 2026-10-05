---
name: iot-benchmark-development
description: Develop or debug IoT Benchmark core tests, configuration loading and database adapters. Use for these code changes, not ordinary documentation edits or unrelated repositories.
---

# IoT Benchmark development

Read only the section needed for the change. Shared workflow and verification commands are in the repository-root [AGENTS.md](../../../AGENTS.md). Source paths below are relative to the repository root; Java package paths start at `core/src/main/java/cn/edu/tsinghua/iot/benchmark/`.

## Core tests

Tests that directly or transitively initialize `ConfigDescriptor` must extend `BenchmarkTestBase` in `core/src/test/java/cn/edu/tsinghua/iot/benchmark/`. It sets `benchmark-conf` before subclass static fields run. Otherwise the module working directory may lack `function.xml`, and `Config.initInnerFunction()` can terminate the test JVM with `System.exit(0)`.

Set configuration before loading classes that capture the singleton in static fields. Restore mutated configuration and reset shared workload state where needed so tests remain order-independent. A successful process exit with missing results is not sufficient evidence; check that the intended tests ran.

Add regression coverage for changed behavior, not assertions that merely duplicate implementation. Follow the root verification guidance to select and stop checks.

## Configuration changes

Wire a new parameter through `conf/Config.java` (field/accessors/default), `conf/ConfigDescriptor.java` (`loadProps()` parsing and validation as applicable), and `configuration/conf/config.properties` (documented example/default). Loading uses explicit setters, not automatic reflection. Test that the supplied property changes effective configuration; cover invalid input when the parameter adds validation.

Update user-facing configuration documentation when semantics change. Keep machine-specific endpoints and credentials out of shared defaults.

## Database adapters

Consult `docs/DeveloperGuide.md` and a comparable adapter. Check `tsdb/IDatabase.java`, the enums under `tsdb/enums/`, `conf/Constants.java` and `tsdb/DBFactory.java`. The factory reflectively instantiates adapters through a constructor accepting `DBConfig`.

For a new adapter, wire the applicable database/version/insert-mode enum values, class-name constant and factory case; register the module in `pom.xml` and supply its assembly descriptor. Check the resulting archive includes the shared configuration. Preserve explicit unsupported-operation behavior instead of reporting false success.

For IoTDB 2.0, inspect model and DML strategy implementations for the affected dialect and insertion path. Validate shared-contract changes across affected adapters, not just the first implementation.

## Benchmark execution

Use the README for package startup and `-cf` configuration. Before an authorized run, inspect the selected target, workload size and deletion settings. Use an isolated test target or the user-specified scope. Capture configuration, versions and measured results for performance comparisons; compilation or a smoke run alone does not establish performance.
