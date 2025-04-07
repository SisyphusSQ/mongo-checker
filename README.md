# mongo-checker

mongo-checker is used to compare two MongoDB databases and find differences between them.  
Offer a data synchronization tool called [mongoshake](https://github.com/SisyphusSQ/MongoShake/tree/dev) which is optimized and fixed few bugs to syncing data from mongo replset or sharding cluster to another mongodb cluster or other databases.

## Overview
![overview](docs/overview.jpg)

## Usage
use config.toml or command line options to specify options.
```bash
$ ./mongo-checker run -h
Start mongodb checker

Usage:
  mongo-checker run [flags]

Examples:
mongo-checker run -c --config <config file>


Flags:
  -c, --config string          config file path (default "./test.toml")
      --conn-mode string       _connect_mode should in [primary, secondaryPreferred, secondary, nearest, standalone] (default "primary")
      --cpuprofile file        write cpu profile to file
      --db-trans string        transform from source database A to destination database B
                               ex: A:B,C:D,...
      --debug                  If debug_mode is true, print debug logs
  -d, --destination string     E.g., mongodb://username:password@primaryA,secondaryB,secondaryC
      --exclude-colls string   which collection(s) should be include, include_coll and exclude_coll are mutually exclusive.
                               ex: coll1 or coll1,coll2,...
      --exclude-dbs string     which database(s) should be include, include_dbs and exclude_dbs are mutually exclusive.
                               ex: db1 or db1,db2,...
  -h, --help                   help for run
      --include-colls string   which collection(s) should be include, include_coll and exclude_coll are mutually exclusive.
                               ex: coll1 or coll1,coll2,...
      --include-dbs string     which database(s) should be include, include_dbs and exclude_dbs are mutually exclusive.
                               ex: db1 or db1,db2,...
      --limit-qps int          Number of rows to checker in one second time (default 5000)
      --log-path string        log and sqlite db file path (default "./logs")
      --memprofile file        write memory profile to file
      --parallel int           Number of collections will be checked in parallel (default 8)
  -s, --source string          E.g., mongodb://username:password@primaryA,secondaryB,secondaryC

```
  
mongo-checker tools can print progress to stdout and log file, like this:
![progress](docs/progress.png)


## TODO
- [ ] Add more data types to _id field