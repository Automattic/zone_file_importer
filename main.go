package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"bufio"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"path/filepath"

	"github.com/jlaffaye/ftp"
	"github.com/miekg/dns"
)

var jobs = make(chan *ftp.Entry)
var output = make(chan string)
var ftpHost = os.Getenv("zf_ftp_host")
var username = os.Getenv("zf_user")
var password = os.Getenv("zf_pass")
var workers = 5

func main() {
	var wg sync.WaitGroup
	go writer()

	entries := filterEntries( zoneList() )

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(&wg)
	}

	for _, entry := range entries {
		jobs <- entry
	}

	close(jobs)

	wg.Wait()

	close(output)
}

func worker(wg *sync.WaitGroup) {
	var conn, ftpErr = ftp.Dial(ftpHost)

	if ftpErr != nil {
		log.Fatal(ftpErr)
	}

	conn.Login(username, password)

	for job := range jobs {
		downloadZone(job, conn)
	}
	wg.Done()
}

func parseZone(entry *ftp.Entry, data io.Reader) {
	// Should add the tld as the second param in case origin is not set
	zp := dns.NewZoneParser(data, "", "")
	
	for rr, ok := zp.Next(); ok; rr, ok = zp.Next() {
		output <- rr.String()
	}

	if zp.Err() != nil {
		fmt.Println("Got error from NewZoneParser::Next for " + entry.Name)
		fmt.Println(zp.Err())
	}
}

func filterEntries(entries []*ftp.Entry) []*ftp.Entry {
	baseNames := make(map[string]bool)

	// First pass: Populate the map with base names and whether they have a .gz extension
	for _, entry := range entries {
		ext := filepath.Ext(entry.Name)
		base := strings.TrimSuffix(entry.Name, ext)

		if ( ext == ".gz" ) {
			baseNames[base] = true
		} else {
			baseNames[base + ext] = false || baseNames[base + ext]
		}
	}

	// Second pass: Filter the entries based on the map
	filtered := make([]*ftp.Entry, 0, len(entries))

	for _, entry := range entries {
		if ( entry.Name == "." || entry.Name == ".." ) {
			continue
		}

		ext := filepath.Ext(entry.Name)
		base := strings.TrimSuffix(entry.Name, ext)

		// Keep the entry if it has a .gz extension or if there's no .gz version for the base name
		if ext == ".gz" || baseNames[base + ext] == false {
			filtered = append(filtered, entry)
		}
	}

	return filtered
}

func zoneList() []*ftp.Entry {
	var conn, ftpErr = ftp.Connect(ftpHost)

	defer conn.Quit()

	if ftpErr != nil {
		log.Fatal(ftpErr)
	}

	conn.Login(username, password)

	entries, err := conn.List("/zonefiles")

	if err != nil {
		log.Fatal(err)
	}

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Size > entries[j].Size
	})
	
	return entries
}

func downloadZone(entry *ftp.Entry, conn *ftp.ServerConn) {
	fmt.Println("Downloading zone: " + entry.Name + "\n")
	resp, err := conn.Retr("/zonefiles/" + entry.Name)

	defer resp.Close()

	isGzFile := strings.HasSuffix( entry.Name, ".gz")

	if err != nil {
		fmt.Println("Got error from conn.Retr for " + entry.Name)
		fmt.Println(err)
		return
	}

	if isGzFile == true {
		reader, e := gzip.NewReader(resp)
		if e != nil {
			fmt.Println("Got error from gzip Reader for " + entry.Name)
			fmt.Println(e)
			return
		}
		parseZone(entry, reader)
	} else {
		reader := bufio.NewReader(resp)
		parseZone(entry, reader)
	}
}

func writer() {
	file, err := os.Create("results.txt")

	if err != nil {
		log.Fatal("Cannot create file", err)
	}

	for {
		line, ok := <-output
		if ok {
			file.WriteString(strings.ToLower(line) + "\n")
		} else {
			file.Close()
			return
		}

	}
}
