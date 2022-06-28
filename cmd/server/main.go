package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"net"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"
)

type LoggingInfo struct {
	hosts    []string
	username string
	password string
	proxy    string
}

func main() {
	proxyFlag := flag.String("x", "proxy.com", "Cassandra proxy")
	hostsFlag := flag.String("h", "127.0.0.1:9042", "Cassandra seeds")
	usernameFlag := flag.String("u", "cassandra", "Cassandra username")
	passwordFlag := flag.String("p", "cassandra", "Cassandra password")
	flag.Parse()

	splitHosts := strings.Split(*hostsFlag, ",")
	loggingInfo := LoggingInfo{
		hosts:    splitHosts,
		username: *usernameFlag,
		password: *passwordFlag,
		proxy:    *proxyFlag,
	}

	log.Printf("%v", loggingInfo)

	var session *gocql.Session
	session, err := createSession(loggingInfo)
	if err != nil {
		panic(err)
	}

	defer session.Close()

	if err = printTable(context.Background(), session, "SELECT * FROM system.peers_v2", gocql.One); err != nil {
		log.Fatal(err)
	}
}

func createSession(loggingInfo LoggingInfo) (*gocql.Session, error) {
	cluster := gocql.NewCluster(loggingInfo.hosts...)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: loggingInfo.username,
		Password: loggingInfo.password,
	}
	cluster.AddressTranslator = MyAddressTranslator(loggingInfo.proxy)
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return session, nil
}

func printTable(ctx context.Context, session *gocql.Session, stmt string, c gocql.Consistency, values ...interface{}) error {
	iter := session.Query(stmt, values...).Consistency(c).WithContext(ctx).Iter()
	fmt.Println(stmt)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	for i, columnInfo := range iter.Columns() {
		if i > 0 {
			_, _ = fmt.Fprint(w, "\t| ")
		}
		_, _ = fmt.Fprintf(w, "%s (%s)", columnInfo.Name, columnInfo.TypeInfo)
	}

	for {
		rd, err := iter.RowData()
		if err != nil {
			return err
		}
		if !iter.Scan(rd.Values...) {
			break
		}
		_, _ = fmt.Fprint(w, "\n")
		for i, val := range rd.Values {
			if i > 0 {
				_, _ = fmt.Fprint(w, "\t| ")
			}

			_, _ = fmt.Fprint(w, reflect.Indirect(reflect.ValueOf(val)).Interface())
		}
	}

	_, _ = fmt.Fprint(w, "\n")
	_ = w.Flush()
	fmt.Println()

	return iter.Close()
}

func MyAddressTranslator(proxy string) gocql.AddressTranslator {

	var resolvedIp net.IP = nil
	ips, _ := net.LookupIP(proxy)
	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			resolvedIp = ipv4
			break
		}
	}

	return gocql.AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		log.Printf("gocql from custom translator: translating address '%v:%d' to '%v:%d'", addr, port, resolvedIp, port)
		return resolvedIp, port
	})
}
