# influxdb-migrate
Tool to migrate between [Influxdb](https://github.com/influxdb/influxdb) database versions.

# Why
Although in beta releases, Influxdb already had a lot of features that were essencial for a project I am working on. It was installed a while ago and the data that was collected couldn't be lost.

# How it works
It retrieves the data direct from the database files (meta information and the data itself) and send the points to the new version using the current client. This way we don't have to learn the new database structure, just the one we are migrating from.

Another bonus of using the client is that the downtime is minimal. You just:
* Stop the old version
* Rename the old data folder
* Upgrade and start the new version
* Start the migration. New points can be collected while you are still performing the migration.

The tool has configurations to wait between writes and to limit the total points per write to control the load on the server.

The structure of the old database is self contained in one file for the version being read from. This will easy the implement of new migrations.

The migration will create all databases, retention policies and points from the old database.

# How to use it
You will need a valid Go installation (at least 1.4).

`go get github.com/vladlopes/influxdb-migrate`

After building (or installing), use the switch -h to see the parameters for the command.

# Limitations
* Only have migration from 0.9.0-rc31 (although I think the database has the same structure since 0.9.0-rc12)
* Don't import Continuous Queries
* Don't use access information (user/password) for the destination database
