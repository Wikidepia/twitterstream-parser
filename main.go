package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"

	"github.com/dsnet/compress/bzip2"
	"github.com/korovkin/limiter"
)

func WalkMatch(root, pattern string) ([]string, error) {
	var matches []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if matched, err := filepath.Match(pattern, filepath.Base(path)); err != nil {
			return err
		} else if matched {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func process(infile string, fout *os.File) {
	// Open json file
	bzip_file, err := os.Open(infile)
	if err != nil {
		log.Println(err)
		return
	}
	defer bzip_file.Close()

	bzip_reader := io.Reader(bzip_file)
	unbzip_reader, err := bzip2.NewReader(bzip_reader, &bzip2.ReaderConfig{})
	if err != nil {
		log.Println(err)
		return
	}

	// Read json and process it
	scanner := bufio.NewScanner(unbzip_reader)
	for scanner.Scan() {
		if err_scan := scanner.Err(); err_scan != nil {
			log.Println(err)
			return
		}
		scanText := scanner.Text()
		matchText := re.FindStringSubmatch(scanText)
		if len(matchText) != 0 {
			if matchText[1] == "in" || matchText[1] == "id" {
				fout.WriteString(scanText + "\n")
			}
		}
	}
	println("Processed", infile)
}

var re = regexp.MustCompile(`(?m)"lang":"(.*?)"`)

func main() {
	indir := flag.String("indir", "stream", "Input file (directory)")
	outfile := flag.String("outfile", "outsample.jsonl", "Output file (.jsonl)")
	flag.Parse()

	// Open output file
	fout, err := os.OpenFile(*outfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	fileArray, _ := WalkMatch(*indir, "*.bz2")
	limit := limiter.NewConcurrencyLimiter(8)
	for _, infile := range fileArray {
		limit.Execute(func() {
			process(infile, fout)
		})
	}
	defer fout.Close()
	limit.Wait()
}
