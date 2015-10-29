package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/models"
	"github.com/vladlopes/influxdb-migrate/database"
	"github.com/vladlopes/influxdb-migrate/from090"
	"github.com/vladlopes/influxdb-migrate/from090rc31"
	"github.com/vladlopes/influxdb-migrate/from092"
)

var (
	versions = map[string]func(string, chan<- database.Database, chan<- client.BatchPoints){
		"090rc31": from090rc31.GetPoints,
		"090":     from090.GetPoints,
		"092":     from092.GetPoints,
	}
	fromversion = flag.String(
		"fromversion",
		"090rc31",
		fmt.Sprintf("From wich version to migrate (%s)", getversions()))
	datapath       = flag.String("datapath", "/home/vagrant/.influxdbold/data", "Location of the old version meta file and shards directory")
	writeurl       = flag.String("writeurl", "http://localhost:8086/", "Url of the new database version")
	betweenwrites  = flag.Duration("betweenwrites", 10*time.Millisecond, "Interval to wait between writes")
	pointsperwrite = flag.Int("pointsperwrite", 500, "Points per write")
	onlyprint      = flag.Bool("onlyprint", false, "Only print points to stdout instead of sending to the server")
	nodbcmd        = flag.Bool("nodbcmd", false, "Don't perform database commands")
)

func main() {
	flag.Parse()

	if *pointsperwrite < 1 {
		log.Fatalf("Invalid points per write. Must be at least 1")
	}

	cdatabases := make(chan database.Database)
	cpoints := make(chan client.BatchPoints)
	if f, ok := versions[*fromversion]; !ok {
		log.Fatalf("Invalid version %s. Valids: %s", *fromversion, getversions())
	} else {
		go f(*datapath, cdatabases, cpoints)
	}

	var c *client.Client
	if !*onlyprint {
		u, err := url.Parse(*writeurl)
		if err != nil {
			log.Fatalf("Invalid url to write %s: %v\n", *writeurl, err)
		}
		c, err = client.NewClient(client.Config{
			URL:       *u,
			UserAgent: "influxdb-migrate",
		})
		if err != nil {
			log.Fatalf("Couldn't create client to write: %v\n", err)
		}
		_, toversion, err := c.Ping()
		if err != nil {
			log.Fatalf("Couldn't connect to server at %v: %v\n", writeurl, err)
		}
		fmt.Printf("Destination server version: %s\n", toversion)
	}

	fmt.Printf("Starting migration from version %s...\n", *fromversion)

	for db := range cdatabases {
		dbcreatecmd := fmt.Sprintf("create database %s", db.Name)
		if *onlyprint {
			fmt.Printf("%s\n", dbcreatecmd)
		} else if !*nodbcmd {
			_, err := c.Query(client.Query{Command: dbcreatecmd})
			if err != nil {
				fmt.Printf("Error creating database %s: %v\n", db.Name, err)
			}
			sleep()
		}
		for _, rp := range db.Policies {
			var def string
			if rp.Name == db.DefaultRetentionPolicy {
				def = "default"
			}
			rpcreatecmd := fmt.Sprintf("create retention policy %s on %s duration %du replication %d %s",
				rp.Name, db.Name, rp.Duration.Nanoseconds()/int64(time.Microsecond), rp.ReplicaN, def)
			if *onlyprint {
				fmt.Printf("%s\n", rpcreatecmd)
			} else if !*nodbcmd {
				_, err := c.Query(client.Query{Command: rpcreatecmd})
				if err != nil {
					fmt.Printf("Error creating retention policy %s on database %s: %v\n", rp.Name, db.Name, err)
				}
				sleep()
			}
		}
	}

	for bp := range cpoints {
		max := *pointsperwrite
		points := bp.Points
		for {
			if len(points) < 1 {
				break
			}
			if len(points) < max {
				max = len(points)
			}
			bp.Points = points[:max]
			if !*onlyprint {
				fmt.Printf(".")
				_, err := c.Write(bp)
				if err != nil {
					fmt.Printf("Error writing batch points %v: %v\n", bp, err)
				}
			} else {
				for _, p := range points {
					if sp, err := models.NewPoint(p.Measurement, p.Tags, p.Fields, p.Time); err != nil {
						fmt.Printf("Error marshalling point %v to line protocol: %v\n", p, err)
					} else {
						fmt.Printf("%s\n", sp)
					}
				}
			}
			points = points[max:]
			sleep()
		}
	}

	fmt.Printf("\nMigration completed!\n")
}

func getversions() string {
	b := &bytes.Buffer{}
	for k := range versions {
		b.WriteString(fmt.Sprintf("[%s]", k))
	}
	return b.String()
}

func sleep() {
	if *betweenwrites > 0 {
		time.Sleep(*betweenwrites)
	}
}
