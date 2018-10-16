package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/jlaffaye/ftp"
	"github.com/miekg/dns"
)

var jobs = make(chan *ftp.Entry)
var output = make(chan string)
var ftpHost = os.Getenv("ftp_host")
var username = os.Getenv("zf_user")
var password = os.Getenv("zf_pass")

func main() {
	var wg sync.WaitGroup
	go writer()

	entries := zoneList()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go worker(&wg)
	}

	for _, entry := range entries {
		jobs <- entry
	}

	wg.Wait()
	output <- "BREAK"
}

func worker(wg *sync.WaitGroup) {
	for job := range jobs {
		downloadZone(job)
	}
	wg.Done()
}

func parseZone(entry *ftp.Entry, data io.Reader, conn *ftp.ServerConn) {
	// Should add the tld as the second param in case origin is not set
	parsed := dns.ParseZone(data, "", "")
	for x := range parsed {
		if x.Error != nil {
			fmt.Println(x.Error)
			continue
		}

		output <- x.RR.String()
	}

	conn.Quit()
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

func downloadZone(entry *ftp.Entry) {
	conn, err := ftp.Connect(ftpHost)
	if err != nil {
		fmt.Println(err)
		return
	}

	conn.Login(username, password)

	if err != nil {
		fmt.Println(err)
		return
	}
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

	parseZone(entry, reader, conn)
}

func writer() {
	file, err := os.Create("results.txt")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}

	for {
		line := <-output
		if line == "BREAK" {
			break
		}
		file.WriteString(line + "\n")
	}

	file.Close()
}
