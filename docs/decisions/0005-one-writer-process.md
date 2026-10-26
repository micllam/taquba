# ADR-0005: One process writes to a store, and others can read or send requests

Status: accepted

Date: 2026-10-20

Scope: taquba

Supersedes: ADR-0001

## Rule

Run every worker of a `Queue` in the process that opened it, and write to its
store only from that process. Another process can read from the store through
`QueueReader`, and can send an `enqueue`, `cancel` or `signal` request through
the inbox, a dedicated prefix outside the store. The owning process applies each
request.

## Context

SlateDB allows one writer per store, and a second writer that opens the store
fences the first. The owning process therefore makes every durable transition:
it runs the workers, and it applies every change to the store. A request from
elsewhere enters as an object in the inbox, so its sender needs write access to
the inbox prefix alone and communicates with the owning process through the
object store.

## Alternatives rejected

- A worker fleet over one store. It would require a second writer, which SlateDB
  fences.
- A network endpoint on the owning process. That process would need a reachable
  port.

## Consequences

- Concurrency comes from tasks in the owning process, and scale-out comes from
  sharding across queues and stores.
- Another process observes a store only through `QueueReader`, which never
  writes, and it reads the outcome of its request there: the inbox is one-way.
