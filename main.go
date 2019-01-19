package main

import (
	"crypto/tls"
	"fmt"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/server"
	"log"
	"net"
)

var proxy *client.Conn
var conn *server.Conn

func main() {
	log.Println("Starting test mysql proxy on localhost:4000")
	l, _ := net.Listen("tcp", "127.0.0.1:4000")

	c, _ := l.Accept()

	var err error
	proxy, err = client.Connect("localhost:3306", "root", "root", "foo")
	if err != nil {
		log.Fatal(err)
	}

	caPem, caKey := generateCA()
	certPem, keyPem := generateAndSignRSACerts(caPem, caKey)
	tlsConf := NewServerTLSConfig(caPem, certPem, keyPem, tls.VerifyClientCertIfGiven)

	svr := server.NewServer(
		"8.0.13",
		mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD,
		getPublicKeyFromCert(certPem),
		tlsConf,
	);
	credProvider := server.NewInMemoryProvider()
	credProvider.AddUser("root", "")
	conn, _ = server.NewCustomizedConn(c, svr, credProvider, TestHandler{})

	for {
		_ = conn.HandleCommand()
	}
}

type TestHandler struct {
}

func (h TestHandler) UseDB(dbName string) error {
	return proxy.UseDB(dbName);
}
func (h TestHandler) HandleQuery(query string) (*mysql.Result, error) {
	log.Printf("Handling Query for %s:  %s\n", conn.GetUser(), query)
	r, err := proxy.Execute(query)
	if err != nil {
		log.Printf("Error for %s in query %s", conn.GetUser(), err)
		return nil, err
	}
	return r, err
}

func (h TestHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, fmt.Errorf("not supported now")
}
func (h TestHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	log.Printf("Preparing statement %s\n", query)
	stmt, err := proxy.Prepare(query)
	if err != nil {
		return 0, 0, nil, err
	}
	return stmt.ParamNum(), stmt.ColumnNum(), stmt, nil
}
func (h TestHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	log.Printf("Executing statement %s\n", query)
	return context.(*client.Stmt).Execute(args)
}

func (h TestHandler) HandleStmtClose(context interface{}) error {
	log.Printf("Executing close statment\n")
	return context.(*client.Stmt).Close()
}

func (h TestHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}
