# ADR-0005: Requests from other processes enter through the inbox

Status: accepted

Date: 2026-10-20

Scope: taquba

Supersedes: ADR-0001

## Rule

Run every worker of a `Queue` in the owning process, the process that opened the
`Queue`, and write to its store only from that process. Another process can send
a request, such as an enqueue, as a JSON object at a versioned path with the
inbox prefix, which is outside the store. The owning process applies each
request in one transaction of the store that also records the request.

## Context

A store is the SlateDB database at a path. SlateDB allows only one writer per
store, and a second writer that opens the store fences the first. Another
process can read from the store through `QueueReader`. A write from it, such as
an enqueue, would open the store as a second writer.

The path of a request identifies its kind. The queue applies the kinds
`enqueue`, `cancel` and `signal`.

The owning process periodically lists the requests in the inbox, applies each
one and then deletes its object. The deletion follows the commit, so a later
listing can return a request that the process has applied.

The inbox is one-way: the sending process reads the outcome of its request
through `QueueReader`.

## Alternatives rejected

- A worker fleet over one store. It would require a second writer, which SlateDB
  fences.

## Consequences

- Concurrency comes from tasks in the owning process, and scale-out comes from
  sharding across queues and stores.
- The code that applies a kind must commit the record of a request in the
  transaction of the request's effect, or apply the request idempotently. A
  request listed twice then takes effect once.
- The owning process applies the requests in id order and stops at the first
  request that fails. A request that fails every time blocks the later requests
  until an operator deletes its object.
- A removed or renamed field requires a new version in the path, and the owning
  process then leaves the requests of the older version unread.
