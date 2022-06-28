module gocassandra

go 1.18

replace github.com/gocql/gocql => ../gocql

require github.com/gocql/gocql v0.0.0-00010101000000-000000000000

require (
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)
