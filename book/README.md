# The taquba book

Built with [mdBook](https://rust-lang.github.io/mdBook/).

```bash
cargo install mdbook   # once
mdbook serve --open    # run from this directory; rebuilds on every edit
mdbook build           # renders into book/
```

Chapters are reached only through `src/SUMMARY.md`, so a new page is added
there in the same edit that creates it.
