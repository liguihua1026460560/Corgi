package main

import (
	mspebble "Corgi/internal/database/pebble"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/encoding/protowire"
)

func main() {
	var (
		dbPath    string
		prefix    string
		prefixHex string
		showValue bool
		strOutput bool
		limit     int
		protoWire bool
	)

	flag.StringVar(&dbPath, "db", "", "Pebble DB directory path")
	flag.StringVar(&prefix, "prefix", "", "scan keys with this string prefix")
	flag.StringVar(&prefixHex, "prefix-hex", "", "scan keys with this hex prefix (example: 0a01ff)")
	flag.BoolVar(&showValue, "show-value", true, "print value bytes")
	flag.BoolVar(&strOutput, "string", false, "print key/value as quoted string instead of hex")
	flag.IntVar(&limit, "limit", 0, "max rows to print, 0 means no limit")

	flag.BoolVar(&protoWire, "proto", false, "decode value as generic protobuf wire format")
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

	if err := run(dbPath, matchPrefix, showValue, strOutput, limit, protoWire); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(dbPath string, prefix []byte, showValue, strOutput bool, limit int, protoWire bool) error {
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

		if protoWire {
			printProtoWire(k, iter.Value())
		} else if showValue {
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
		if len(v) == 8 {
			fmt.Printf("key=%q value=%q le-int64=%d\n", k, v, int64(binary.LittleEndian.Uint64(v)))
			return
		}

		fmt.Printf("key=%q value=%q\n", k, v)
		return
	}
	if len(v) == 8 {
		fmt.Printf("key=%q value=%d\n", k, int64(binary.LittleEndian.Uint64(v)))
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

func printProtoWire(k, v []byte) {
	s, err := formatProtoWireObject(v)
	if err != nil {
		fmt.Printf("key=%q value={decode_error:%q}\n", k, err.Error())
		return
	}

	fmt.Printf("key=%q value=%s\n", k, s)
}

func formatProtoWireObject(data []byte) (string, error) {
	var b strings.Builder

	b.WriteByte('{')

	first := true

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return "", protowire.ParseError(n)
		}
		data = data[n:]

		value, used, err := formatProtoWireValue(typ, data)
		if err != nil {
			return "", fmt.Errorf("field %d: %w", num, err)
		}
		data = data[used:]

		if !first {
			b.WriteByte(',')
		}
		first = false

		b.WriteString(strconv.FormatInt(int64(num), 10))
		b.WriteByte(':')
		b.WriteString(value)
	}

	b.WriteByte('}')
	return b.String(), nil
}

func formatProtoWireValue(typ protowire.Type, data []byte) (string, int, error) {
	switch typ {
	case protowire.VarintType:
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return "", 0, protowire.ParseError(n)
		}
		return strconv.FormatUint(v, 10), n, nil

	case protowire.Fixed32Type:
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return "", 0, protowire.ParseError(n)
		}
		return strconv.FormatUint(uint64(v), 10), n, nil

	case protowire.Fixed64Type:
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return "", 0, protowire.ParseError(n)
		}
		return strconv.FormatUint(v, 10), n, nil

	case protowire.BytesType:
		v, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return "", 0, protowire.ParseError(n)
		}

		// string 字段
		if isPrintableUTF8(v) {
			return strconv.Quote(string(v)), n, nil
		}

		// repeated int64 / repeated uint64 这类 packed varint 字段
		if vals, ok := tryPackedVarints(v); ok {
			return formatUint64Array(vals), n, nil
		}

		// 不知道是什么 bytes，又不想打印 hex，就只打印长度
		return fmt.Sprintf("<bytes len=%d>", len(v)), n, nil

	default:
		return "", 0, fmt.Errorf("unsupported wire type %v", typ)
	}
}

func isPrintableUTF8(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	if !utf8.Valid(b) {
		return false
	}

	runes := []rune(string(b))
	if len(runes) == 0 {
		return false
	}

	printable := 0
	for _, r := range runes {
		if r == '\n' || r == '\r' || r == '\t' || (r >= 0x20 && r != 0x7f) {
			printable++
		}
	}

	return printable*100/len(runes) >= 90
}

func tryPackedVarints(b []byte) ([]uint64, bool) {
	if len(b) == 0 {
		return nil, false
	}

	// 防止普通字符串被误判成 packed varint
	if isPrintableUTF8(b) {
		return nil, false
	}

	var vals []uint64
	rest := b

	for len(rest) > 0 {
		v, n := protowire.ConsumeVarint(rest)
		if n < 0 {
			return nil, false
		}
		vals = append(vals, v)
		rest = rest[n:]
	}

	return vals, true
}

func formatUint64Array(vals []uint64) string {
	var b strings.Builder

	b.WriteByte('[')
	for i, v := range vals {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(v, 10))
	}
	b.WriteByte(']')

	return b.String()
}
