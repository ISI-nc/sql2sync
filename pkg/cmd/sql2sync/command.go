package sql2sync

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"net"
	"os"

	"github.com/spf13/cobra"

	streamquery "github.com/mcluseau/sql2sync/pkg/stream-query"
	client "github.com/mcluseau/sql2sync/pkg/sync2kafka-client" // FIXME create a real package
)

var (
	sq = &streamquery.StreamQuery{}

	target             string
	token              string
	topic              string
	useTLS             bool
	caCert             string
	insecureSkipVerify bool

	syncInit = &client.SyncInitInfo{Format: "binary"}
)

func New() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql2sync [query to send]",
		Short: "Send an SQL query's results to a sync2kafka instance",
		Long:  ``,
		Run:   run,
	}

	flag := cmd.Flags()

	flag.StringVar(&target, "target", "localhost:9084", "Target server")
	flag.BoolVar(&useTLS, "tls", false, "Use TLS to connect to server")
	flag.BoolVar(&insecureSkipVerify, "insecure-skip-verify", false, "Skip TLS identity verification")
	flag.StringVar(&caCert, "ca-cert", "", "Trusted certificate(s) for TLS identity verification")

	flag.StringVar(&syncInit.Token, "token", "", "Authn token for the target server")
	flag.StringVar(&syncInit.Topic, "topic", "", "Target topic")
	flag.BoolVar(&syncInit.DoDelete, "do-delete", false, "Instruct server to perform deletions too")

	sq.BindFlags(flag)

	return cmd
}

func run(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		log.Fatal("The query is required.")
	}
	if len(sq.KeyColumns) == 0 {
		log.Fatal("One or more keys are required.")
	}

	// connect to target
	var (
		conn net.Conn
		err  error
	)

	if !useTLS {
		conn, err = net.Dial("tcp", target)
	} else {
		cfg := &tls.Config{
			InsecureSkipVerify: insecureSkipVerify,
		}

		if len(caCert) != 0 {
		}

		conn, err = tls.Dial("tcp", target, cfg)
	}

	if err != nil {
		log.Fatal("failed to connect to server: ", err)
	}

	defer conn.Close()
	log.Print("connected to server")

	// start the query
	sq.Query = args[0]

	kvs := make(chan streamquery.KeyValue, 10)
	go sq.StreamTo(kvs)

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	if err = enc.Encode(syncInit); err != nil {
		log.Fatal(err)
	}

	for kv := range kvs {
		if err = enc.Encode(client.BinaryKV{
			Key:   kv.Key,
			Value: kv.Value,
		}); err != nil {
			log.Fatal(err)
		}
	}

	log.Print("end of data")
	if err = enc.Encode(client.BinaryKV{EndOfTransfer: true}); err != nil {
		log.Fatal(err)
	}

	log.Print("waiting for result...")

	result := client.SyncResult{}
	if err = dec.Decode(&result); err != nil {
		log.Fatal(err)
	}

	log.Print("result: ", result.OK)

	if !result.OK {
		os.Exit(1)
	}
}
