# Security

The Local-first Durable Job Queue is a local-first tool.

It has no authentication or authorization layer.

The web interface is read-only.

It does not mutate the queue.

Run the web interface on localhost when the database is sensitive.

```text
jobqueue web -addr 127.0.0.1:8080 -db queue.db
```

The database may contain private job payloads.

Protect the database file with the same rules as your other secrets.

## Reporting

Report a security issue in the GitHub issue tracker.

Do not include secret material in the report.

Describe the problem and the version you found it in.

