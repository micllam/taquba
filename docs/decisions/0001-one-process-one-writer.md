# ADR-0001: Producers and workers share one process

Status: accepted
Date: 2026-09-14
Scope: taquba

## Rule

Every producer and worker for a `Queue` runs in the process that opened the
`Queue`. Multi-node worker fleets and out-of-process producers are out of scope.

## Context

A store is the SlateDB database at one path. SlateDB allows one writer per
store, and a second writer that opens the store fences the first. The
single-writer property comes from the substrate and is not a choice made here.
Every durable transition therefore runs in the process that opened the store's
`Queue`.

## Alternatives rejected

- A multi-node worker fleet over one store. It requires a second writer, which
  the substrate refuses.
- Out-of-process producers, for the same reason.

## Consequences

- Concurrency comes from tasks in one process, and scale-out comes from
  sharding across queues and stores.
- Observation from another process goes through `QueueReader`, which never
  writes.
- The writer heartbeat exists because an external observer cannot otherwise
  distinguish a live writer from a writer whose process terminated.
