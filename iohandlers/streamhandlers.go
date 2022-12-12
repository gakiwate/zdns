/*
 * ZDNS Copyright 2022 Regents of the University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package iohandlers

import (
	"bufio"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/nsqio/go-nsq"
)

type NSQStreamInputHandler struct {
	consumer nsq.Consumer
}

func NewNSQStreamInputHandler(topic string, channel string) *NSQStreamInputHandler {
	// Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(topic, channel, config)
	consumer.SetLoggerLevel(nsq.LogLevelError)
	if err != nil {
		log.Fatal(err)
	}

	return &NSQStreamInputHandler{
		consumer: *consumer,
	}
}

func (h *NSQStreamInputHandler) FeedChannel(in chan<- interface{}, wg *sync.WaitGroup) error {
	defer close(in)
	defer (*wg).Done()

	// Set the Handler for messages received by this Consumer. Can be called multiple times.
	// See also AddConcurrentHandlers.
	h.consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		// handle the message
		in <- string(m.Body)
		return nil
	}))

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err := h.consumer.ConnectToNSQLookupd("localhost:4161")
	if err != nil {
		log.Fatal(err)
	}

	// wait for signal to exit
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Gracefully stop the consumer.
	h.consumer.Stop()

	return nil
}

type NSQStreamOutputHandler struct {
	producer  nsq.Producer
	nsq_topic string
}

func NewNSQStreamOutputHandler(nsq_topic string) *NSQStreamOutputHandler {
	// Create a new NSQ producer
	producer, err := nsq.NewProducer("localhost:4150", nsq.NewConfig())
	producer.SetLoggerLevel(nsq.LogLevelError)
	if err != nil {
		// Report Error and Exit.
		log.Fatal(err)
	}

	return &NSQStreamOutputHandler{
		producer:  *producer,
		nsq_topic: nsq_topic,
	}
}

func (h *NSQStreamOutputHandler) WriteResults(results <-chan string, wg *sync.WaitGroup) error {
	defer (*wg).Done()
	for n := range results {
		h.producer.Publish(h.nsq_topic, []byte(n))
	}
	return nil
}

type StreamInputHandler struct {
	reader io.Reader
}

func NewStreamInputHandler(r io.Reader) *StreamInputHandler {
	return &StreamInputHandler{
		reader: r,
	}
}

func (h *StreamInputHandler) FeedChannel(in chan<- interface{}, wg *sync.WaitGroup) error {
	defer close(in)
	defer (*wg).Done()

	s := bufio.NewScanner(h.reader)
	for s.Scan() {
		in <- s.Text()
	}
	if err := s.Err(); err != nil {
		log.Fatalf("unable to read input stream: %v", err)
	}
	return nil
}

type StreamOutputHandler struct {
	writer io.Writer
}

func NewStreamOutputHandler(w io.Writer) *StreamOutputHandler {
	return &StreamOutputHandler{
		writer: w,
	}
}

func (h *StreamOutputHandler) WriteResults(results <-chan string, wg *sync.WaitGroup) error {
	defer (*wg).Done()
	for n := range results {
		io.WriteString(h.writer, n+"\n")
	}
	return nil
}
