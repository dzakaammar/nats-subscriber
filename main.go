package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/kumparan/tapao"

	"github.com/kumparan/kumnats/v2"
	stan "github.com/nats-io/go-nats-streaming"
)

var cluster, clientID, natsURL, subject string
var subs stan.Subscription

type Message struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Time string `json:"time"`
}

func (m *Message) ParseFromBytes(data []byte) error {
	return tapao.Unmarshal(data, &m, tapao.FallbackWith(tapao.JSON))
}

func main() {
	flag.StringVar(&cluster, "cluster", "", "nats streaming cluster")
	flag.StringVar(&clientID, "clientID", "", "nats streaming clientID")
	flag.StringVar(&natsURL, "url", "", "nats streaming url")
	flag.StringVar(&subject, "subject", "", "subject")

	flag.Parse()

	n, err := kumnats.NewNATSWithCallback(
		cluster,
		clientID,
		natsURL,
		natsSubscriberConnectCallback(),
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if subs != nil {
			_ = subs.Unsubscribe()
		}
		_ = n.Close()
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	<-signalChan
	time.Sleep(2 * time.Second)
}

func natsSubscriberConnectCallback() kumnats.NatsCallback {
	return func(nats kumnats.NATS) {
		s, err := nats.Subscribe(subject, kumnats.NewNATSMessageHandler(new(Message), 1, 1*time.Second, func(msg kumnats.MessagePayload) error {
			data, ok := msg.(*Message)
			if !ok {
				return fmt.Errorf("error casting nats message\n")
			}

			fmt.Printf("Got message. ID : %s, Type: %s, Time: %s\n", data.ID, data.Type, data.Time)
			return nil
		}), stan.SetManualAckMode())

		if err != nil {
			log.Fatal(err)
		}
		subs = s
		fmt.Println("Subscribing....")
	}
}
