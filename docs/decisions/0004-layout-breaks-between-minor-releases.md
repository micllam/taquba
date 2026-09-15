# ADR-0004: The on-disk layout can break between minor releases

Status: accepted
Date: 2026-09-15
Scope: workspace

## Rule

Before 1.0, a minor release can break the on-disk layout, and a patch release
preserves the layout. Do not add migration code or a read path for an older
layout.

## Context

The on-disk layout (the key layout and the record layouts) still changes
between releases. Cargo's version rules apply to the public API of a crate, and
the data that the crate stores needs a rule of its own.

During a drain, workers process every queued job before the upgrade, so the
next minor release opens a store without jobs.

## Alternatives rejected

- A layout version with migration code, or a fallback read of the older layout,
  to keep a store readable across a minor release. With either, every later
  layout change would also keep each earlier migration or fallback working. A
  drain before the upgrade removes the need for both.

## Re-open triggers

Either of two events re-opens the decision: the 1.0 release, or documentation
that recommends the KV namespace for application state.

## Consequences

An upgrade across minor releases can require a drain. A design that needs a
flag to keep an old layout readable is rejected.
