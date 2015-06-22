package from090

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/influxql"
	mi "github.com/influxdb/influxdb/meta/internal"
	ti "github.com/influxdb/influxdb/tsdb/internal"
	"github.com/vladlopes/influxdb-migrate/database"
)

type replaceescaped struct {
	newtoken string
	replaced string
}

var (
	escapes = map[string]replaceescaped{
		`\,`: replaceescaped{newtoken: `§_§a§`, replaced: `,`},
		`\"`: replaceescaped{newtoken: `§_§b§`, replaced: `"`},
		`\ `: replaceescaped{newtoken: `§_§c§`, replaced: ` `},
		`\=`: replaceescaped{newtoken: `§_§d§`, replaced: `=`},
	}
)

type measurement struct {
	Name   string
	Fields []field
}

type field struct {
	ID   uint8
	Name string
	Type influxql.DataType
}

func GetPoints(datapath string,
	cdatabases chan<- database.Database,
	cpoints chan<- client.BatchPoints) {

	metapath := filepath.Join(datapath, "meta/raft.db")

	meta, err := bolt.Open(
		metapath,
		0600,
		&bolt.Options{Timeout: 1 * time.Second, ReadOnly: true})

	if err != nil {
		log.Fatalf("Error opening raft database from %s: %v\n", metapath, err)
	}

	var databases []database.Database
	err = meta.View(func(tx *bolt.Tx) error {
		logs := tx.Bucket([]byte("logs"))
		if logs == nil {
			log.Fatalf("Error opening logs bucket: %v\n", err)
		}
		err = logs.ForEach(func(k, v []byte) error {
			l := new(raft.Log)
			decodeMsgPack(v, l)
			databases = applycommand(databases, l.Data)
			return nil
		})
		if err != nil {
			log.Fatalf("Error traversing raft logs: %v\n", err)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Error reading raft database: %v\n", err)
	} else {
		err = meta.Close()
		if err != nil {
			log.Fatalf("Error closing raft database: %v\n", err)
		}
	}

	for _, db := range databases {
		cdatabases <- db
	}
	close(cdatabases)

	for _, db := range databases {
		for _, rp := range db.Policies {
			shardspath := filepath.Join(datapath, "data", db.Name, rp.Name)
			shards, err := ioutil.ReadDir(shardspath)
			if err != nil {
				continue
			}
			for _, sf := range shards {
				shdb, err := bolt.Open(
					filepath.Join(shardspath, sf.Name()),
					0600,
					&bolt.Options{Timeout: 1 * time.Second, ReadOnly: true})
				if err != nil {
					log.Fatalf("Error opening shard %s from rp %s on database %s: %v\n",
						sf.Name(), rp.Name, db.Name, err)
				}
				err = shdb.View(func(tx *bolt.Tx) error {
					measurements := make(map[string]*measurementFields)
					fb := tx.Bucket([]byte("fields"))
					if fb == nil {
						log.Fatalf("Couldn't find bucket fields in shard %s", sf.Name())
					}
					fb.ForEach(func(k, v []byte) error {
						mname := string(k)
						mf := &measurementFields{}
						err := mf.UnmarshalBinary(v)
						if err != nil {
							log.Fatalf("Error unmarshalling measurement %s: %v\n", mname, err)
						}
						measurements[mname] = mf
						return nil
					})
					tx.ForEach(func(name []byte, b *bolt.Bucket) error {
						bname := string(name)
						if bname != "fields" && bname != "series" {
							bnameescaped := bname
							for k, v := range escapes {
								bnameescaped = strings.Replace(bnameescaped, k, v.newtoken, -1)
							}
							bnamesplitted := strings.Split(bnameescaped, ",")
							mname := bnamesplitted[0]
							if _, ok := measurements[mname]; !ok {
								fmt.Printf("Couldn't find measurement %s in measurements", mname)
							} else {
								tags := make(map[string]string)
								for i := 1; i < len(bnamesplitted); i++ {
									ts := strings.Split(bnamesplitted[i], "=")
									tag := ts[1]
									for _, v := range escapes {
										tag = strings.Replace(tag, v.newtoken, v.replaced, -1)
									}
									tags[ts[0]] = tag
								}

								bp := client.BatchPoints{
									Database:        db.Name,
									RetentionPolicy: rp.Name,
								}
								b.ForEach(func(k, v []byte) error {
									bp.Points = append(bp.Points, client.Point{
										Measurement: mname,
										Time:        time.Unix(0, int64(btou64(k))),
										Tags:        tags,
										Fields:      getfields(mname, measurements[mname], v),
									})
									return nil
								})
								cpoints <- bp
							}
						}
						return nil
					})
					return nil
				})

				if err != nil {
					log.Fatalf("Error traversing shard %s from rp %s on database %s: %v\n",
						sf.Name(), rp.Name, db.Name, err)
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

func getfields(mname string, m *measurementFields, b []byte) (ret map[string]interface{}) {
	ret = make(map[string]interface{})
	for {
		if len(b) < 1 {
			break
		}
		fid := b[0]
		var f *field
		for _, mf := range m.Fields {
			if mf.ID == fid {
				f = mf
				break
			}
		}
		if f.ID == 0 {
			log.Fatalf("Couldn't find field %d in measurement %s\n", fid, mname)
		}
		var value interface{}
		switch f.Type {
		case influxql.Float:
			value = math.Float64frombits(binary.BigEndian.Uint64(b[1:9]))
			b = b[9:]
		case influxql.Integer:
			value = int64(binary.BigEndian.Uint64(b[1:9]))
			b = b[9:]
		case influxql.Boolean:
			if b[1] == 1 {
				value = true
			} else {
				value = false
			}
			b = b[2:]
		case influxql.String:
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

func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func applycommand(dbs []database.Database, b []byte) []database.Database {
	var cmd mi.Command
	if err := proto.Unmarshal(b, &cmd); err != nil {
		return dbs
	}
	updateddbs := dbs
	switch cmd.GetType() {
	case mi.Command_CreateDatabaseCommand:
		ext, _ := proto.GetExtension(&cmd, mi.E_CreateDatabaseCommand_Command)
		v := ext.(*mi.CreateDatabaseCommand)
		updateddbs = append(updateddbs, database.Database{Name: v.GetName()})
	case mi.Command_DropDatabaseCommand:
		ext, _ := proto.GetExtension(&cmd, mi.E_DropDatabaseCommand_Command)
		v := ext.(*mi.DropDatabaseCommand)
		if len(dbs) > 0 {
			updateddbs = make([]database.Database, len(dbs)-1)
			for _, db := range dbs {
				if db.Name != v.GetName() {
					updateddbs = append(updateddbs, database.Database{Name: db.Name})
				}
			}
		}
	case mi.Command_CreateRetentionPolicyCommand:
		ext, _ := proto.GetExtension(&cmd, mi.E_CreateRetentionPolicyCommand_Command)
		v := ext.(*mi.CreateRetentionPolicyCommand)
		for i, db := range updateddbs {
			if db.Name == v.GetDatabase() {
				rp := v.GetRetentionPolicy()
				updateddbs[i].Policies = append(updateddbs[i].Policies,
					database.RetentionPolicy{
						Name:     rp.GetName(),
						Duration: time.Duration(rp.GetDuration()),
						ReplicaN: rp.GetReplicaN(),
					})
				break
			}
		}
	case mi.Command_DropRetentionPolicyCommand:
		ext, _ := proto.GetExtension(&cmd, mi.E_DropRetentionPolicyCommand_Command)
		v := ext.(*mi.DropRetentionPolicyCommand)
		for i, db := range updateddbs {
			if db.Name == v.GetDatabase() {
				if len(updateddbs[i].Policies) > 0 {
					updateddbs[i].Policies = make([]database.RetentionPolicy, len(updateddbs[i].Policies)-1)
					for _, rp := range updateddbs[i].Policies {
						if rp.Name != v.GetName() {
							updateddbs[i].Policies = append(updateddbs[i].Policies,
								database.RetentionPolicy{
									Name:     rp.Name,
									Duration: rp.Duration,
									ReplicaN: rp.ReplicaN,
								})
						}
					}
				}
				break
			}
		}
	case mi.Command_SetDefaultRetentionPolicyCommand:
		ext, _ := proto.GetExtension(&cmd, mi.E_SetDefaultRetentionPolicyCommand_Command)
		v := ext.(*mi.SetDefaultRetentionPolicyCommand)
		for i, db := range updateddbs {
			if db.Name == v.GetDatabase() {
				updateddbs[i].DefaultRetentionPolicy = v.GetName()
				break
			}
		}
	}
	return updateddbs
}

type measurementFields struct {
	Fields map[string]*field `json:"fields"`
}

// MarshalBinary encodes the object to a binary format.
func (m *measurementFields) MarshalBinary() ([]byte, error) {
	var pb ti.MeasurementFields
	for _, f := range m.Fields {
		id := int32(f.ID)
		name := f.Name
		t := int32(f.Type)
		pb.Fields = append(pb.Fields, &ti.Field{ID: &id, Name: &name, Type: &t})
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes the object from a binary format.
func (m *measurementFields) UnmarshalBinary(buf []byte) error {
	var pb ti.MeasurementFields
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	m.Fields = make(map[string]*field)
	for _, f := range pb.Fields {
		m.Fields[f.GetName()] = &field{ID: uint8(f.GetID()), Name: f.GetName(), Type: influxql.DataType(f.GetType())}
	}
	return nil
}

func (m *measurementFields) String() string {
	b := &bytes.Buffer{}
	b.WriteString("[")
	for _, f := range m.Fields {
		b.WriteString(fmt.Sprintf("%d %s %v |", f.ID, f.Name, f.Type))
	}
	b.WriteString("]")
	return b.String()
}
