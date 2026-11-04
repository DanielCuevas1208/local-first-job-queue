package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/local-first-job-queue/internal/archive"
)

// Verify reads an exported history archive without opening the live queue.
// The archive verifier restores records into memory and reports every issue.
func Verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	inPath := fs.String("in", "-", "JSONL input path; - reads standard input")
	jsonOutput := fs.Bool("json", false, "output the verification report as JSON")
	fs.Parse(args)

	if fs.NArg() > 0 {
		return fmt.Errorf("usage: verify [-in path] [-json]")
	}

	input := io.Reader(os.Stdin)
	var file *os.File
	if *inPath != "-" {
		var err error
		file, err = os.Open(*inPath)
		if err != nil {
			return fmt.Errorf("open archive: %w", err)
		}
		defer file.Close()
		input = file
	}

	report, err := archive.Verify(input)
	if err != nil {
		return err
	}
	if err := writeVerificationReport(os.Stdout, report, *jsonOutput); err != nil {
		return fmt.Errorf("write verification report: %w", err)
	}
	if !report.Valid() {
		return fmt.Errorf("archive verification failed with %d issue(s)", len(report.Issues))
	}
	return nil
}

type verificationOutput struct {
	Records      int             `json:"records"`
	ValidRecords int             `json:"valid_records"`
	Valid        bool            `json:"valid"`
	Issues       []archive.Issue `json:"issues,omitempty"`
	Warnings     []archive.Issue `json:"warnings,omitempty"`
}

func writeVerificationReport(w io.Writer, report archive.VerificationReport, jsonOutput bool) error {
	if jsonOutput {
		return json.NewEncoder(w).Encode(verificationOutput{
			Records:      report.Records,
			ValidRecords: report.ValidRecords,
			Valid:        report.Valid(),
			Issues:       report.Issues,
			Warnings:     report.Warnings,
		})
	}

	fmt.Fprintf(w, "Archive verification\n")
	fmt.Fprintf(w, "  records:  %d\n", report.Records)
	fmt.Fprintf(w, "  valid:    %d\n", report.ValidRecords)
	fmt.Fprintf(w, "  warnings: %d\n", len(report.Warnings))
	fmt.Fprintf(w, "  issues:   %d\n", len(report.Issues))
	for _, warning := range report.Warnings {
		fmt.Fprintf(w, "  warning line %d [%s]", warning.Line, warning.Code)
		if warning.JobID != "" {
			fmt.Fprintf(w, " job %s", warning.JobID)
		}
		fmt.Fprintf(w, ": %s\n", warning.Message)
	}
	for _, issue := range report.Issues {
		fmt.Fprintf(w, "  issue line %d [%s]", issue.Line, issue.Code)
		if issue.JobID != "" {
			fmt.Fprintf(w, " job %s", issue.JobID)
		}
		fmt.Fprintf(w, ": %s\n", issue.Message)
	}
	if report.Valid() {
		fmt.Fprintln(w, "status: valid")
	} else {
		fmt.Fprintln(w, "status: invalid")
	}
	return nil
}
