package main

import (
	"log"
	"os"

	"github.com/local-first-job-queue/internal/cli"
)

func main() {
	log.SetFlags(0)
	if err := cli.Web(os.Args[1:]); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
