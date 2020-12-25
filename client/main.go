package main

import (
	"context"
	"flag"
	"time"

	pb "github.com/joshikeno/go/grpc/server/proto"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	serverAddr = flag.String("server_addr", "localhost:50091", "the server address")
)

func checkBalance(client pb.TransactionsClient, token *pb.Token) (*pb.Summary, error) {
	logrus.Println("get latest transaction balance for token %v", token)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	summary, err := client.GetBalance(ctx, token)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to GetBalance", client)
	}

	return summary, nil
}

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logrus.Fatalf("failed to dail: %v", err)
	}
	defer conn.Close()

	client := pb.NewTransactionsClient(conn)

	sum, err := checkBalance(client, &pb.Token{Id: "zambam"})
	if err != nil {
		logrus.Fatal(errors.Unwrap(err))
	}

	logrus.Println(sum)
}
