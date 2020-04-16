package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/kumparan/tapao"

	"github.com/kumparan/kumnats"
	stan "github.com/nats-io/go-nats-streaming"
)

var cluster, clientID, natsURL, subject string
var subs stan.Subscription

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
		s, err := nats.Subscribe(subject, func(msg *stan.Msg) {
			m := new(kumnats.NatsMessage)
			err := tapao.Unmarshal(msg.Data, m, tapao.FallbackWith(tapao.JSON))
			if err != nil {
				log.Println(err)
				return
			}

			fmt.Printf("Got message. ID : %d, Type: %s, Time: %s\n", m.ID, m.Type, m.Time)
		})

		if err != nil {
			log.Fatal(err)
		}
		subs = s
		fmt.Println("Subscribing....")
	}
}
