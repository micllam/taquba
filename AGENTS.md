# AGENTS.md

Rules for agents working in this repository. A crate's own `AGENTS.md` adds the
rules for that crate.

## Checks

Before reporting a change as done, run the commands of the `lint` and `test`
jobs in `.github/workflows/ci.yml`. Run `vale sync` after a clone.

## Invariants

- **Delivery.** Do not add an exactly-once mode. See
  [ADR-0002](docs/decisions/0002-at-least-once-delivery.md).
- **State.** Design the key layout before adding durable state. Do not add a
  durable copy of the lease without a consumer outside the writing process.
- **Ingress.** A request from outside the process must enter through the inbox,
  and only the writer may commit it. The inbox kinds must stay `enqueue`,
  `cancel` and `signal`, and a layer must receive its requests as jobs. Do not
  add a reply path or a second channel. See
  [ADR-0005](docs/decisions/0005-one-writer-process.md).
- **Signals.** The signal wait must stay the queue's, and a signal must address
  the waiting job by id. Do not add a signal key, a waiter index, a signal
  buffer or a delivered record.

## Versioning

Do not add migration code or a read path for an older on-disk layout. See
[ADR-0004](docs/decisions/0004-layout-breaks-between-minor-releases.md).

A new enum variant, struct field or function parameter in the public API must go
into a minor release. Do not add a compatibility alias or `#[non_exhaustive]`.

A release must follow [RELEASE.md](RELEASE.md).

## Book

The book in `book/src/` is part of the public surface. A change to what a page
states must update the page in the same commit.
