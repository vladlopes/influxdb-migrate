package database

import "time"

type Database struct {
	Name                   string
	DefaultRetentionPolicy string
	Policies               []RetentionPolicy
}

type RetentionPolicy struct {
	Name     string
	Duration time.Duration
	ReplicaN uint32
}
