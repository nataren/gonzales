package main

// Send a bunch of messages to AWS Kinesis
import (
	kinesis "github.com/sendgridlabs/go-kinesis"
	"log"
	"flag"
	"fmt"
)

func main() {

	// Read command arguments
	var streamName string
	flag.StringVar(&streamName, "streamname", "", "The name of the AWS Kinesis stream")

	// Validate the arguments
	if streamName == "" {
		log.Fatal("The streanname is a required argument")
	}

	// Read the env variables
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	secretKey := os.Getenv("AWS_SECRET_KEY")
	regionName := os.Getenv("AWS_REGION_NAME)
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

	// Batch insert records
	args := kinesis.NewArgs()
	args.Add("StreamName", streamName)
	for i := 0; i < 10; i++ {
		args.AddRecord(
		[]byte(),
		fmt.Sprintf("partitionKey-%d", i))
	}
	resp, err := kinesis.PutRecords(args)
	if err != nil {
		fmt.Printf("PutRecords err: %v\n", err)
	} else {
		fmt.Printf("PutRecords: %v\n", resp)
	}
	fmt.Println("Batch sent messages to AWS Kinesis")
}
