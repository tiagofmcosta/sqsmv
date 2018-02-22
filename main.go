package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//go:generate go clean -i
//go:generate golint -set_exit_status
//go:generate go vet -v
//go:generate dep ensure -v
//go:generate go install -v

func main() {
	maxWorkers := 35

	src := flag.String("src", "", "source queue")
	dest := flag.String("dest", "", "destination queue")
	workers := flag.Int("workers", 25, fmt.Sprintf("number of workers (MAX: %v)", maxWorkers))
	flag.Parse()

	if *src == "" || *dest == "" {
		flag.Usage()
		os.Exit(1)
	}

	if *workers > maxWorkers {
		flag.Usage()
		os.Exit(1)
	}

	if os.Getenv("AWS_REGION") == "" {
		fmt.Printf("AWS_REGION not set")
		os.Exit(1)
	}

	region := os.Getenv("AWS_REGION")

	config := &aws.Config{
		Region: &region,
	}

	log.Printf("source queue : %v", *src)
	log.Printf("destination queue : %v", *dest)

	maxMessages := int64(10)
	waitTime := int64(0)
	messageAttributeNames := aws.StringSlice([]string{"All"})

	var wg1 sync.WaitGroup
	wg1.Add(*workers)

	for i := 1; i <= *workers; i++ {
		go func(i int) {
			defer wg1.Done()

			awsSession, _ := session.NewSession()

			client := sqs.New(awsSession, config)

			rmin := &sqs.ReceiveMessageInput{
				QueueUrl:              src,
				MaxNumberOfMessages:   &maxMessages,
				WaitTimeSeconds:       &waitTime,
				MessageAttributeNames: messageAttributeNames,
			}

			// loop as long as there are messages on the queue
			for {
				resp, err := client.ReceiveMessage(rmin)

				if err != nil {
					panic(err)
				}

				if len(resp.Messages) == 0 {
					log.Printf("[Worker-%v] done", i)
					return
				}

				log.Printf("[Worker-%v] received %v messages...", i, len(resp.Messages))

				var wg sync.WaitGroup
				wg.Add(len(resp.Messages))

				for _, m := range resp.Messages {
					go func(m *sqs.Message) {
						defer wg.Done()

						// write the message to the destination queue
						smi := sqs.SendMessageInput{
							MessageAttributes: m.MessageAttributes,
							MessageBody:       m.Body,
							QueueUrl:          dest,
						}

						_, err := client.SendMessage(&smi)

						if err != nil {
							log.Printf("[Worker-%v] ERROR sending message to destination %v", i, err)
							return
						}

						// message was sent, dequeue from source queue
						dmi := &sqs.DeleteMessageInput{
							QueueUrl:      src,
							ReceiptHandle: m.ReceiptHandle,
						}

						if _, err := client.DeleteMessage(dmi); err != nil {
							log.Printf("[Worker-%v] ERROR dequeueing message ID %v : %v", i,
								*m.ReceiptHandle,
								err)
						}
					}(m)
				}

				// wait for all jobs from this batch...
				wg.Wait()

				time.Sleep(time.Second * 1)
			}
		}(i)

		time.Sleep(time.Second * 1)
	}

	wg1.Wait()
}
