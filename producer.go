package main

// Send a bunch of messages to AWS Kinesis
import (
	"flag"
	"fmt"
	kinesis "github.com/sendgridlabs/go-kinesis"
	"log"
	"os"
	"sync"
	"time"
)

func main() {

	// Read command arguments
	var streamName string
	flag.StringVar(&streamName, "streamName", "", "The name of the AWS Kinesis stream")
	var sleepRange int64
	flag.Int64Var(&sleepRange, "sleepRange", 1, "The milliseconds to sleep between requests")
	var producers uint
	flag.UintVar(&producers, "producers", 1, "The number of concurrent producers")
	var eventCount int
	flag.IntVar(&eventCount, "eventcount", 10, "The number of events to send")
	flag.Parse()

	// Validate the arguments
	if streamName == "" {
		log.Fatal("The streamName is a required argument")
	}

	// Read the env variables
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	secretKey := os.Getenv("AWS_SECRET_KEY")
	regionName := os.Getenv("AWS_REGION_NAME")
	if accessKey == "" {
		log.Fatal("The AWS_ACCESS_KEY env variable needs to be set")
	}
	if secretKey == "" {
		log.Fatal("The AWS_SECRET_KEY env variable needs to be set")
	}
	if regionName == "" {
		log.Fatal("The AWS_REGION_NAME env variable needs to be set")
	}

	// Auth
	auth := kinesis.NewAuth()
	auth.InferCredentialsFromEnv()
	k := kinesis.New(&auth, kinesis.Region{})

	// Loop forever
	log.Printf("Will produce data to stream %v, using %v producers, sleeping %v ms in between", streamName, producers, sleepRange)
	var wg sync.WaitGroup

	for p := uint(0); p < producers; p++ {
		wg.Add(1)
		go func(n uint) {
			defer wg.Done()
			for i := 0; i < eventCount; i++ {
				time.Sleep(time.Duration(sleepRange) * time.Millisecond)

				// Batch insert records
				args := kinesis.NewArgs()
				args.Add("StreamName", streamName)
				args.AddRecord(
					[]byte(fmt.Sprintf(EVENT, i)),
					fmt.Sprintf("site_%d", i))

				_, err := k.PutRecords(args)

				if err != nil {
					fmt.Printf("Error @ PutRecords, worker %v: %v\n\n", n, err)
				} else {
					fmt.Printf("Success PutRecords, worker %v, iteration %v\n", n, i)
				}
			}
		}(p)
	}
	wg.Wait()
}

const (
	EVENT = `<event id="a3352360-8ec1-11e3-a063-8e4856e73110" datetime="2014-02-06T00:00:06Z" type="page:view" wikiid="site_1" journaled="false" version="2"> <request id="a3352360-8ec1-11e3-a063-0eb7d2788566" seq="1" count="1"><signature>POST:events/page-view/*</signature><ip>127.0.0.1</ip><session-id>005d41fa-f608-4bee-9db7-27da808100c0</session-id><parameters /><user id="2" anonymous="true" /></request><page id="1"><path>Page Path</path></page><data><_uri.host>host.io</_uri.host><_uri.scheme>https</_uri.scheme><_uri.query>refer=support</_uri.query><serial_number>%d</serial_number></data></event>`
	LIMIT = 10
)
