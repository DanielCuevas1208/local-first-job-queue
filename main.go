package main

import (
	"fmt"
	"log"
	"os"

	"github.com/local-first-job-queue/internal/cli"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("")

	if len(os.Args) < 2 {
		fmt.Println(`Local-first Durable Job Queue

Commands:
  enqueue  Add a job to the queue
  work     Start a worker process
  inspect  View queue state and event log
  history  View the event log for one job
  requeue  Return a dead-lettered job to the queue
  seed     Load bundled sample data
  metrics  Expose queue state for Prometheus
  web      Serve the inspection dashboard
  demo     Run a self-contained scenario with fault injection

Use <command> -help for command flags.`)
		return
	}

	sub := os.Args[1]
	args := os.Args[2:]

	var err error
	switch sub {
	case "enqueue":
		err = cli.Enqueue(args)
	case "work":
		err = cli.Work(args)
	case "inspect":
		err = cli.Inspect(args)
	case "history":
		err = cli.History(args)
	case "requeue":
		err = cli.Requeue(args)
	case "seed":
		err = cli.Seed(args)
	case "metrics":
		err = cli.Metrics(args)
	case "web":
		err = cli.Web(args)
	case "demo":
		err = cli.Demo(args)
	default:
		fmt.Printf("unknown command: %s\n", sub)
		os.Exit(1)
	}
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
