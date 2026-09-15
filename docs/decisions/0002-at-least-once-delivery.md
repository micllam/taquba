# ADR-0002: Delivery is at-least-once

Status: accepted
Date: 2026-09-14
Scope: taquba

## Rule

Workers must be idempotent. There is no exactly-once mode and there will not be
one.

## Context

A worker can finish its work and fail before the settlement commits. The lease
then expires and the job is claimed again. Recovery at open does the same for
every claim of a terminated process.

Exactly-once delivery across a queue and an external effect requires that
effect to commit inside the queue's transaction. The queue cannot include an
arbitrary effect in that transaction, so the queue does not guarantee
exactly-once delivery.

## Alternatives rejected

- An exactly-once delivery mode. It requires every external effect to commit in
  the queue's transaction.

## Consequences

- Every worker must tolerate a repeated delivery.
- State the queue does own can be made exact, because a KV write in
  `SettlementEffects` commits in the settlement transaction.
- A dedup key deduplicates an enqueue and does not deduplicate a delivery.
