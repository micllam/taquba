# Decision records

One record per decision that constrains a future change.

## Conventions

- A record is immutable once accepted. A decision that changes gets a new
  record with a `Supersedes: ADR-NNNN` line, and the old one becomes
  `Status: superseded by ADR-NNNN`.
- A decision gets a record when it is significant or hard to reverse. A
  significant decision affects the structure, the non-functional qualities, the
  dependencies, the public interfaces or the construction techniques of the
  project. A decision local to a single change does not get a record, and its
  rationale goes in that change's commit message.
- A record covers one decision in about a page. It states the decision, the
  reasons for it and the alternatives already rejected, and the code docs
  describe how the code implements it. It does not record the discussion that
  produced it, when an alternative was tried or how often.
- A record describes this project only. It compares against architectural
  categories and never against a named product.

## Template

```markdown
# ADR-NNNN: Title

Status: accepted
Date: YYYY-MM-DD
Scope: <crate or workspace>
Supersedes: <ADR-NNNN, or delete this line>

## Rule

The constraint, in one or two imperative sentences.

## Context

What makes the decision necessary, in one or two paragraphs.

## Alternatives rejected

One bullet each, with the reason it fails.

## Re-open triggers

Optional. The finding that re-opens the decision.

## Consequences

What a future change must accept.
```
