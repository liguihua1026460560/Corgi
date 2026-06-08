package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"github.com/cockroachdb/pebble/v2"
	mspebble "mossserver/internal/com/macrosan/database/pebble"
	"os"
)

func main() {
	var (
		dbPath    string
		prefix    string
		prefixHex string
		showValue bool
		strOutput bool
		limit     int
	)

	flag.StringVar(&dbPath, "db", "", "Pebble DB directory path")
	flag.StringVar(&prefix, "prefix", "", "scan keys with this string prefix")
	flag.StringVar(&prefixHex, "prefix-hex", "", "scan keys with this hex prefix (example: 0a01ff)")
	flag.BoolVar(&showValue, "show-value", true, "print value bytes")
	flag.BoolVar(&strOutput, "string", false, "print key/value as quoted string instead of hex")
	flag.IntVar(&limit, "limit", 0, "max rows to print, 0 means no limit")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: peppledump -db <db_dir> [options]\n\n")
		fmt.Fprintln(flag.CommandLine.Output(), "Examples:")
		fmt.Fprintln(flag.CommandLine.Output(), "  peppledump -db /data/pebble")
		fmt.Fprintln(flag.CommandLine.Output(), "  peppledump -db /data/pebble -prefix user:")
		fmt.Fprintln(flag.CommandLine.Output(), "  peppledump -db /data/pebble -prefix-hex 0a01ff -limit 100")
		fmt.Fprintln(flag.CommandLine.Output(), "")
		flag.PrintDefaults()
	}
	flag.Parse()

	if dbPath == "" {
		fmt.Fprintln(os.Stderr, "error: -db is required")
		flag.Usage()
		os.Exit(2)
	}
	if prefix != "" && prefixHex != "" {
		fmt.Fprintln(os.Stderr, "error: only one of -prefix and -prefix-hex can be set")
		os.Exit(2)
	}

	matchPrefix, err := parsePrefix(prefix, prefixHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid prefix: %v\n", err)
		os.Exit(2)
	}

	if err := run(dbPath, matchPrefix, showValue, strOutput, limit); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(dbPath string, prefix []byte, showValue, strOutput bool, limit int) error {
	db, err := pebble.Open(dbPath, &pebble.Options{ReadOnly: true, Merger: mspebble.NewMossMerger()})
	if err != nil {
		return fmt.Errorf("open pebble db %q: %w", dbPath, err)
	}
	defer db.Close()

	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	var (
		count int
		ok    bool
	)

	if len(prefix) > 0 {
		ok = iter.SeekGE(prefix)
	} else {
		ok = iter.First()
	}

	for ; ok; ok = iter.Next() {
		k := iter.Key()
		if len(prefix) > 0 && !bytes.HasPrefix(k, prefix) {
			break
		}

		if showValue {
			printKV(k, iter.Value(), strOutput)
		} else {
			printKey(k, strOutput)
		}

		count++
		if limit > 0 && count >= limit {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterate db: %w", err)
	}

	mode := "full"
	if len(prefix) > 0 {
		mode = "prefix"
	}
	fmt.Fprintf(os.Stderr, "scan done, mode=%s, matched=%d\n", mode, count)
	return nil
}

func parsePrefix(prefix, prefixHex string) ([]byte, error) {
	if prefix != "" {
		return []byte(prefix), nil
	}
	if prefixHex == "" {
		return nil, nil
	}
	if len(prefixHex)%2 != 0 {
		return nil, errors.New("-prefix-hex must have even length")
	}
	b, err := hex.DecodeString(prefixHex)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func printKV(k, v []byte, strOutput bool) {
	if strOutput {
		fmt.Printf("key=%q value=%q\n", k, v)
		return
	}

	fmt.Printf("key=%s value=%s\n", hex.EncodeToString(k), hex.EncodeToString(v))
}

func printKey(k []byte, strOutput bool) {
	if strOutput {
		fmt.Printf("key=%q\n", k)
		return
	}
	fmt.Printf("key=%s\n", hex.EncodeToString(k))
}
