package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"code.google.com/p/go-dbf/godbf"
	"github.com/tgulacsi/go/text"
	"gopkg.in/inconshreveable/log15.v2"
)

var Log = log15.New()

type context struct {
	Csv                                bool
	TablePrefix, DbfEncoding, Encoding string
}

func main() {
	Log.SetHandler(log15.StderrHandler)

	flagTablePrefix := flag.String("prefix", "W_kl_motor_", "table name prefix")
	flagDbfEncoding := flag.String("dbfenc", "iso-8859-2", "DBF file encoding")
	flagEncoding := flag.String("encoding", "iso-8859-2", "file encoding")
	flagCsv := flag.Bool("csv", false, "csv output (otherwise .sql)")
	flag.Parse()

	path := ""
	if flag.NArg() < 1 {
		dh, err := os.Open("")
		if err != nil {
			Log.Crit("cannot open directory", "error", err)
			os.Exit(1)
		}
		fis, err := dh.Readdir(-1)
		dh.Close()
		if err != nil {
			Log.Crit("error listing directory", "path", dh.Name(), "error", err)
			os.Exit(2)
		}
		for _, fi := range fis {
			if !fi.IsDir() {
				continue
			}
			p := filepath.Join(dh.Name(), fi.Name())
			if path < p {
				path = p
			}
		}
		if path == "" {
			Log.Error("cannot find a dir, please specifiy one!")
			os.Exit(3)
		}
	} else {
		path = flag.Arg(0)
	}

	if err := exportDir(path,
		context{Csv: *flagCsv, TablePrefix: *flagTablePrefix,
			DbfEncoding: *flagDbfEncoding, Encoding: *flagEncoding}); err != nil {
		Log.Error("exportDir", "path", path, "error", err)
		os.Exit(4)
	}
}

func exportDir(path string, ctx context) error {
	dh, err := os.Open(path)
	defer dh.Close()
	if err != nil {
		return err
	}
	names, err := dh.Readdirnames(-1)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for _, nm := range names {
		fn := filepath.Join(dh.Name(), nm)
		if strings.ToLower(filepath.Ext(fn)) != ".dbf" {
			continue
		}
		wg.Add(1)
		go func(fn string) {
			defer wg.Done()
			if err = exportFile(fn, ctx); err != nil {
				select {
				case errCh <- fmt.Errorf("exportFile(%q): %v", fn, err):
				default:
				}
			}
		}(fn)
	}
	wg.Wait()
	select {
	case err = <-errCh:
		return err
	default:
		return nil
	}
}

func exportFile(fn string, ctx context) error {
	Log.Debug("exportFile", "fn", fn)
	table, err := godbf.NewFromFile(fn, ctx.DbfEncoding)
	if err != nil {
		return err
	}
	ext := ".sql"
	if ctx.Csv {
		ext = ".csv"
	}
	fh, err := os.Create(filepath.Join(filepath.Dir(fn), stripExt(fn)+ext))
	if err != nil {
		return fmt.Errorf("exportFile create file %s: %v", stripExt(fn)+ext, err)
	}
	defer fh.Close()
	bw := bufio.NewWriter(fh)
	defer bw.Flush()
	var w io.Writer = bw
	if ctx.Encoding != "" {
		w = text.NewWriter(bw, text.GetEncoding(ctx.Encoding))
	}
	if ctx.Csv {
		return exportFileCsv(w, table)
	}
	return exportFileSQL(w, fh.Name(), table, ctx)
}

func exportFileCsv(w io.Writer, table *godbf.DbfTable) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()
	fields := table.Fields()
	Log.Debug("exportFileCsv", "fields", fields)
	fieldNames := make([]string, len(fields))
	for i := range fields {
		fieldNames[i] = fields[i].FieldName()
	}
	return cw.Error()
}

func exportFileSQL(w io.Writer, fileName string, table *godbf.DbfTable, ctx context) error {
	fileName = filepath.Base(fileName)
	nm := ctx.TablePrefix + stripExt(fileName)

	_, err := fmt.Fprintf(w, "TRUNCATE TABLE %s;\n/*\nCREATE TABLE %s (\n", nm, nm)
	if err != nil {
		return fmt.Errorf("error writing to %#v: %v", w, err)
	}
	fields := table.Fields()
	quoters := make([]func(string) string, len(fields))
	fieldNames := make([]string, len(fields))
	Log.Debug("exportFileSQL", "fields", fields)
	for i, f := range fields {
		if i > 0 {
			if _, err = io.WriteString(w, ",\n"); err != nil {
				return err
			}
		}
		if _, err = fmt.Fprintf(w, "  %s %s", f.FieldName(), sqlType(f)); err != nil {
			return err
		}
		fieldNames[i] = f.FieldName()
		quoters[i] = sqlQuoter(f, ctx.DbfEncoding)
	}
	if _, err = io.WriteString(w, "\n) TABLESPACE LDAT;\n*/\n"); err != nil {
		return err
	}

	pre := "INSERT INTO " + nm + " (" + strings.Join(fieldNames, ", ") + ")\n  VALUES ("
	post := ");\n"

	for i := 0; i < table.NumberOfRecords(); i++ {
		io.WriteString(w, pre)
		for j := range fields {
			if j > 0 {
				io.WriteString(w, ", ")
			}
			io.WriteString(w, quoters[j](table.FieldValue(i, j)))
		}
		if _, err = io.WriteString(w, post); err != nil {
			return err
		}
	}
	_, err = io.WriteString(w, "\nCOMMIT;\n")
	return err
}

func sqlType(f godbf.DbfField) string {
	switch f.FieldType() {
	case "C":
		return fmt.Sprintf("VARCHAR2(%d)", f.FieldLength())
	case "N":
		return fmt.Sprintf("NUMBER(%d)", f.FieldLength())
	case "D":
		return "DATE"
	}
	return f.FieldType()
}

func sqlQuoter(f godbf.DbfField, dbfEncoding string) func(string) string {
	switch f.FieldType() {
	case "C":
		return func(x string) string {
			return "'" + strings.Replace(strings.Replace(x,
				"'", "''", -1),
				"&", "||CHR(39)||", -1) + "'"
		}

	case "D":
		return func(x string) string {
			return "TO_DATE('YYYYMMDD', '" + x + "')"
		}
	}
	return func(x string) string { return x }
}

func stripExt(fn string) string {
	fn = filepath.Base(fn)
	ext := filepath.Ext(fn)
	if ext == "" {
		return fn
	}
	return fn[:len(fn)-len(ext)]
}
