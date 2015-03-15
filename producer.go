package main

// Send a bunch of messages to AWS Kinesis
import (
	"flag"
	"fmt"
	kinesis "github.com/sendgridlabs/go-kinesis"
	"log"
	"os"
)

func main() {

	// Read command arguments
	var streamName string
	flag.StringVar(&streamName, "streamName", "", "The name of the AWS Kinesis stream")
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

	// Create stream
	k := kinesis.New(&kinesis.Auth{}, kinesis.Region{})
	err := k.CreateStream(streamName, 1)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Created stream")

	// Batch insert records
	args := kinesis.NewArgs()
	args.Add("StreamName", streamName)
	for i := 0; i < 10; i++ {
		args.AddRecord(
			[]byte(EVENT),
			fmt.Sprintf("partitionKey-%d", i))
	}
	resp, err := k.PutRecords(args)
	if err != nil {
		fmt.Printf("PutRecords err: %v\n", err)
	} else {
		fmt.Printf("PutRecords: %v\n", resp)
	}
	fmt.Println("Batch sent messages to AWS Kinesis")
}

const (
	EVENT = `<event id="a3352360-8ec1-11e3-a063-8e4856e73110" datetime="2014-02-06T00:00:06Z" type="page:view" wikiid="site_1" journaled="false" version="2"> <request id="a3352360-8ec1-11e3-a063-0eb7d2788566" seq="1" count="1"><signature>POST:events/page-view/*</signature><ip>127.0.0.1</ip><session-id>005d41fa-f608-4bee-9db7-27da808100c0</session-id><parameters /><user id="2" anonymous="true" /></request><page id="1"><path>Page Path</path></page><data><_uri.host>host.io</_uri.host><_uri.scheme>https</_uri.scheme><_uri.query>refer=support</_uri.query></data></event>`
)
