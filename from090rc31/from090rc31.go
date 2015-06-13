package from090rc31

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"math"
	"path/filepath"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/client"
	"github.com/vladlopes/influxdb-migrate/database"
)

type versiondb struct {
	Name                   string
	DefaultRetentionPolicy string
	Policies               []versionrp
	Measurements           []*measurement
}

type versionrp struct {
	Name        string
	Duration    time.Duration
	ReplicaN    uint32
	ShardGroups []shardgroup
}

type shardgroup struct {
	Shards []shard
}

type shard struct {
	Id int
}

type measurement struct {
	Name   string
	Fields []field
	Series []serie
}

type field struct {
	Id   byte
	Name string
	Type string
}

type serie struct {
	Id   uint64
	Tags map[string]string
}

func GetPoints(datapath string,
	cdatabases chan<- database.Database,
	cpoints chan<- client.BatchPoints) {

	metapath := filepath.Join(datapath, "meta")

	meta, err := bolt.Open(
		metapath,
		0600,
		&bolt.Options{Timeout: 1 * time.Second, ReadOnly: true})

	if err != nil {
		log.Fatalf("Error opening meta database from %s: %v\n", metapath, err)
	}

	var databases []versiondb
	err = meta.View(func(tx *bolt.Tx) error {
		dbs := tx.Bucket([]byte("Databases"))
		if dbs == nil {
			log.Fatalf("Error opening Databases bucket: %v\n", err)
		}
		err = dbs.ForEach(func(k, v []byte) error {
			dbbucket := dbs.Bucket(k)
			if dbbucket == nil {
				log.Fatalf("Error getting bucket for database %s\n", string(k))
			}
			meta := dbbucket.Get([]byte("meta"))
			if meta == nil {
				log.Fatalf("Error getting meta info for database %s\n", string(k))
			}
			var db versiondb
			err = json.Unmarshal(meta, &db)
			if err != nil {
				log.Fatalf("Error decoding meta info for database %s\n", string(k))
			}
			mb := dbbucket.Bucket([]byte("Measurements"))
			if mb == nil {
				log.Fatalf("Error getting measurements for database %s\n", db.Name)
			}
			mb.ForEach(func(k, v []byte) error {
				var m *measurement
				err = json.Unmarshal(v, &m)
				if err != nil {
					log.Fatalf("Error decoding measurement %s for database %s: %v\n",
						string(k), db.Name, err)
				}
				db.Measurements = append(db.Measurements, m)
				return nil
			})
			sb := dbbucket.Bucket([]byte("Series"))
			if sb == nil {
				log.Fatalf("Error getting series for database %s\n", db.Name)
			}
			for _, m := range db.Measurements {
				b := sb.Bucket([]byte(m.Name))
				if b != nil {
					b.ForEach(func(k, v []byte) error {
						var s serie
						err = json.Unmarshal(v, &s)
						if err != nil {
							log.Fatalf("Error decoding serie %d for measurement %s in database %s: %v\n",
								btou64(k), m.Name, db.Name, err)
						}
						m.Series = append(m.Series, s)
						return nil
					})
				}
			}

			databases = append(databases, db)
			return nil
		})
		if err != nil {
			log.Fatalf("Error traversing databases: %v\n", err)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Error reading meta database: %v\n", err)
	} else {
		err = meta.Close()
		if err != nil {
			log.Fatalf("Error closing meta database: %v\n", err)
		}
	}

	for _, db := range databases {
		rdb := database.Database{
			Name: db.Name,
			DefaultRetentionPolicy: db.DefaultRetentionPolicy,
		}
		for _, rp := range db.Policies {
			rdb.Policies = append(rdb.Policies, database.RetentionPolicy{
				Name:     rp.Name,
				Duration: rp.Duration,
				ReplicaN: rp.ReplicaN,
			})
		}
		cdatabases <- rdb
	}
	close(cdatabases)

	shardspath := filepath.Join(datapath, "shards")
	for _, db := range databases {
		for _, rp := range db.Policies {
			for _, sg := range rp.ShardGroups {
				for _, sh := range sg.Shards {
					shdb, err := bolt.Open(
						filepath.Join(shardspath, strconv.Itoa(sh.Id)),
						0600,
						&bolt.Options{Timeout: 1 * time.Second, ReadOnly: true})
					if err != nil {
						log.Fatalf("Error opening shard %d from rp %s on database %s: %v\n",
							sh.Id, rp.Name, db.Name, err)
					}
					err = shdb.View(func(tx *bolt.Tx) error {
						for _, m := range db.Measurements {
							for _, s := range m.Series {
								sb := tx.Bucket(u64tob(s.Id))
								if sb == nil {
									continue
								}
								bp := client.BatchPoints{
									Database:        db.Name,
									RetentionPolicy: rp.Name,
								}
								sb.ForEach(func(k, v []byte) error {
									bp.Points = append(bp.Points, client.Point{
										Measurement: m.Name,
										Time:        time.Unix(0, int64(btou64(k))),
										Tags:        s.Tags,
										Fields:      getfields(m, v),
									})
									return nil
								})
								cpoints <- bp
							}
						}

						return nil
					})

					if err != nil {
						log.Fatalf("Error traversing shard %d from rp %s on database %s: %v\n",
							sh.Id, rp.Name, db.Name, err)
					}
				}
			}
		}
	}
	close(cpoints)
}

func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func getfields(m *measurement, b []byte) (ret map[string]interface{}) {
	ret = make(map[string]interface{})
	for {
		if len(b) < 1 {
			break
		}
		fid := b[0]
		var f field
		for _, mf := range m.Fields {
			if mf.Id == fid {
				f = mf
				break
			}
		}
		if f.Id == 0 {
			log.Fatalf("Couldn't find field %d in measurement %s\n", fid, m.Name)
		}
		var value interface{}
		switch f.Type {
		case "float":
			value = math.Float64frombits(binary.BigEndian.Uint64(b[1:9]))
			b = b[9:]
		case "integer":
			value = int64(binary.BigEndian.Uint64(b[1:9]))
			b = b[9:]
		case "boolean":
			if b[1] == 1 {
				value = true
			} else {
				value = false
			}
			b = b[2:]
		case "string":
			size := binary.BigEndian.Uint16(b[1:3])
			value = string(b[3 : size+3])
			b = b[size+3:]
		default:
			log.Fatalf("unsupported value type during decode fields: %s", f.Type)
		}
		ret[f.Name] = value
	}
	return
}
