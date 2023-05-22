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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/nsqio/go-nsq"
)

// parse json input
// expects {"domain": "abc.xyz", "metadata": {"sha1": "xxxx"}}
func parseJSONInputLine(line string) (map[string]string, error) {
	var input map[string]interface{}
	err := json.Unmarshal([]byte(line), &input)
	if err != nil {
		log.Error("Malformed Input Line")
		return nil, err
	}
	metadata := map[string]string{}
	for k, v := range input["metadata"].(map[string]interface{}) {
		metadata[k] = v.(string)
	}
	return metadata, nil
}

func checkScanAfterMetadata(metadata map[string]string) int64 {
	scanAfter, _ := strconv.ParseInt(metadata["scan_after"], 0, 64)
	if scanAfter > 0 {
		tnow := time.Now().Unix()
		if tnow < scanAfter {
			return scanAfter - tnow
		}
	}
	return 0
}

type NSQStreamInputHandler struct {
	nsqHost  string
	consumer nsq.Consumer
}

func NewNSQStreamInputHandler(nsqHost string, inTopic string, inChannel string) *NSQStreamInputHandler {
	// Instantiate a consumer that will subscribe to the provided channel.
	consumer, err := nsq.NewConsumer(inTopic, inChannel, nsq.NewConfig())
	consumer.SetLoggerLevel(nsq.LogLevelError)
	if err != nil {
		log.Fatal(err)
	}

	return &NSQStreamInputHandler{
		nsqHost:  nsqHost,
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
		entryMetadata, _ := parseJSONInputLine(string(m.Body))
		tsleep := checkScanAfterMetadata(entryMetadata)
		if tsleep > 0 {
			m.RequeueWithoutBackoff(time.Duration(tsleep) * time.Second)
			log.Debug(m.ID, " is requeue-ing after some seconds: ", tsleep)
			return nil
		}
		in <- string(m.Body)
		return nil
	}))

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	nsqUrl := fmt.Sprintf("%s:4161", h.nsqHost)
	err := h.consumer.ConnectToNSQLookupd(nsqUrl)
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
	producer    nsq.Producer
	nsqOutTopic string
}

func NewNSQStreamOutputHandler(nsqHost string, nsqOutTopic string) *NSQStreamOutputHandler {
	// Create a new NSQ producer
	nsqUrl := fmt.Sprintf("%s:4150", nsqHost)
	producer, err := nsq.NewProducer(nsqUrl, nsq.NewConfig())
	producer.SetLoggerLevel(nsq.LogLevelError)
	if err != nil {
		// Report Error and Exit.
		log.Fatal(err)
	}

	return &NSQStreamOutputHandler{
		producer:    *producer,
		nsqOutTopic: nsqOutTopic,
	}
}

func (h *NSQStreamOutputHandler) WriteResults(results <-chan string, wg *sync.WaitGroup) error {
	defer (*wg).Done()
	for n := range results {
		h.producer.Publish(h.nsqOutTopic, []byte(n))
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
