# influxdb-migrate
Tool to migrate between [Influxdb](https://github.com/influxdb/influxdb) database versions.

# Why
Although still not in a stable 1.0 release, Influxdb already had a lot of features that were essential for a project I am working on. It was installed a while ago and the data that was collected couldn't be lost.

# How it works
It retrieves the data direct from the database files (meta information and the data itself) and send the points to the new version using the current client. This way we don't have to learn the new database structure, just the one we are migrating from.

Another bonus of using the client is that the downtime is minimal. You just:
* Stop the old version
* Rename the old data folder (this will depend on the version you are migrating from)
* Upgrade and start the new version which should create the new data/meta folders and files for the new version
* Start the migration. New points can be collected while you are still performing the migration.

The tool has configurations to wait between writes and to limit the total points per write to control the load on the server.

The structure of the old database is self contained in one file for the version being read from. This will easy the implementation of new migrations.

The migration will create all databases, retention policies if instructed to do so and all points from the old database.

# How to use it
You will need a valid Go installation (at least 1.4).

`go get github.com/vladlopes/influxdb-migrate`

After building (or installing), use the switch -h to see the parameters for the command.

# Limitations
* Don't import Continuous Queries
* Don't use access information (user/password) for the destination database
* Don't use possible raft snapshots to perform database commands, relying only on the `raft.db` file
* If you don't want to issue database and retention policy commands, they must exist in the new database before the migration starts

# Example
Migrating from the latest `b1` and `bz1` engine. The first was introduced in 0.9.0 and the last in 0.9.3. On 0.9.2 the shard format was changed to include the format of the engine used. Therefore, to migrate from the latest version without `tsm1` engine you could do on Ubuntu:

```
sudo stop influxdb
sudo mv /var/opt/influxdb /var/opt/influxdbold
sudo mkdir /var/opt/influxdb
# change the engine on your config file to tsm1
# engine = tsm1
sudo start influxdb
./influxdb-migrate -datapath='/var/opt/influxdbold' -fromversion=092 -pointsperwrite=1000 -betweenwrites=1s
```
