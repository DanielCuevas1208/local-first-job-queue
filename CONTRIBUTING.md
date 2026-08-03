# Contributing

Thanks for improving the Local-first Durable Job Queue.

## Development

Require Go 1.25 or newer.

Format your code before you commit.

```text
gofmt -l .
```

Build every package with this command.

```text
go build ./...
```

Run static checks with this command.

```text
go vet ./...
```

Run the full test suite with this command.

```text
go test -count=1 -race ./...
```

Run queue benchmarks with this command.

```text
go test ./internal/queue -run '^$' -bench Benchmark -benchmem -count=1
```

## Changes

Keep one release slice per pull request.

Add deterministic tests for every behavior you change.

Update the README roadmap and release notes when you add a feature.

Keep the public queue API stable unless a tested correction needs a change.

## Documentation

Write public documentation with ASD-STE100 Issue 9 principles.

Use active voice and short paragraphs.

Use no more than 20 words in instructions.

Use no more than 25 words in descriptive sentences.

Do not use emojis in public documentation.
