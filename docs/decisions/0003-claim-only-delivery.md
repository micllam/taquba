# ADR-0003: Claim-only delivery

Status: accepted
Date: 2026-09-14
Scope: taquba

## Rule

A job reaches a worker only through a pull-based claim transaction.

## Context

A broker routes jobs to the consumers connected to it. `taquba` runs inside the
consumer's process: a consumer takes a job with a claim transaction, and the
consumer decides how many tasks poll and how often.

## Alternatives rejected

- A dispatcher inside `taquba` that claims jobs and sends them to workers over
  in-process channels. It routes jobs to consumers, which makes `taquba` a
  broker.

## Consequences

- A wakeup does not deliver a job, and the woken worker claims the job itself.
