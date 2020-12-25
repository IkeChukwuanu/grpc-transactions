package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"sort"
	"sync"
	"time"

	pb "github.com/joshikeno/go/grpc/server/proto"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	host   = flag.String("host", "0.0.0.0", "server hostname")
	port   = flag.Int("port", 50091, "server port number")
	jsonDB = flag.String("json_db", "./data/transaction_db.json", "json fake db file")
)

type transactions []*pb.Transaction

func (s transactions) Len() int {
	return len(s)
}

func (s transactions) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s transactions) Less(i, j int) bool {
	return s[i].Action.Timestamp.Now < s[j].Action.Timestamp.Now
}

type transactionsServer struct {
	pb.UnimplementedTransactionsServer

	mu sync.RWMutex
	db []*pb.Transaction
}

// load fake db
func (s *transactionsServer) loadDB(filePath string) error {
	var err error
	var data []byte

	if filePath == "" {
		return errors.New("fake db filepath is empty")
	}

	data, err = ioutil.ReadFile(filePath)
	if err != nil {
		return errors.Wrapf(err, "failed to read fake db")
	}
	if err := json.Unmarshal(data, &s.db); err != nil {
		errors.Wrapf(err, "failed to unmarshal fake db")
	}

	return nil
}

func (s *transactionsServer) Debit(ctx context.Context, action *pb.Action) (*pb.Reaction, error) {
	return nil, nil
}

func (s *transactionsServer) Credit(ctx context.Context, action *pb.Action) (*pb.Reaction, error) {
	return nil, nil
}

// func (s *transactionsServer) Transfer(ctx context.Context) {}

func (s *transactionsServer) GetBalance(ctx context.Context, token *pb.Token) (*pb.Summary, error) {
	var trac []*pb.Transaction

	for _, t := range s.db {
		if !proto.Equal(t.Token, token) {
			continue
		}
		trac = append(trac, t)
	}
	if len(trac) <= 0 {
		return nil, errors.New("failed to fetch balance, no transaction found with that token")
	}

	sort.Sort(transactions(trac))

	tt := trac[0]

	return &pb.Summary{
		Balance: tt.Reaction.Balance,
		LastTransaction: &pb.Transaction{
			Token: tt.Token,
			Action: &pb.Action{
				Cash: &pb.Cash{
					Currency: pb.Currency_USD,
					Ammount:  tt.Action.Cash.Ammount,
				},
				Ref:       tt.Action.Ref,
				Type:      tt.Action.Type,
				Timestamp: tt.Action.Timestamp,
			},
			Reaction: &pb.Reaction{
				Status:      tt.Reaction.Status,
				Balance:     &pb.Cash{Ammount: tt.Reaction.Balance.Ammount, Currency: tt.Reaction.Balance.Currency},
				PrevBalance: &pb.Cash{Ammount: tt.Reaction.PrevBalance.Ammount, Currency: tt.Reaction.PrevBalance.Currency},
			},
		},
	}, nil
}

func newServer() *transactionsServer {
	s := &transactionsServer{}
	s.loadDB(*jsonDB)
	return s
}

func main() {
	flag.Parse()

	_, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}

	logrus.Printf("Listening ... on: %v ⚡\n", lis.Addr())

	grpcServer := grpc.NewServer()
	pb.RegisterTransactionsServer(grpcServer, newServer())

	if err := grpcServer.Serve(lis); err != nil {
		errors.Wrapf(err, "failed to server: ")
	}

}
