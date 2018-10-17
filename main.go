package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

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

	entries := zoneList()

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
	var conn, ftpErr = ftp.Connect(ftpHost)

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
	parsed := dns.ParseZone(data, "", "")
	for x := range parsed {
		if x.Error != nil {
			fmt.Println(x.Error)
			continue
		}

		output <- x.RR.String()
	}
}

func zoneList() []*ftp.Entry {
	var conn, ftpErr = ftp.Connect(ftpHost)

	if ftpErr != nil {
		log.Fatal(ftpErr)
	}

	conn.Login(username, password)

	entries, err := conn.List("/zonefiles")

	if err != nil {
		log.Fatal(err)
	}

	conn.Quit()
	return entries
}

func downloadZone(entry *ftp.Entry, conn *ftp.ServerConn) {
	fmt.Println("Downloading zone: " + entry.Name + "\n")
	resp, err := conn.Retr("/zonefiles/" + entry.Name)

	if err != nil {
		fmt.Println(err)
		return
	}

	reader, e := gzip.NewReader(resp)
	if e != nil {
		fmt.Println(err)
		return
	}

	parseZone(entry, reader)
	resp.Close()
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
