package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/url"
)

type Request struct {
	Url       string
	Headers   map[string]string
	Method    string
	Host      string
	AgentId   string
	Timestamp int64
	Postdata  string
}

type Config struct {
	Kafka struct {
		Brokers []string
		Topic   string
		Group   string
	}
	Network struct {
		Proxy string
	}
}

var zeekMsg = [...]string{"Content-Type", "Accept-Encoding", "Referer", "Cookie", "Origin", "Host", "Accept-Language",
	"Accept", "Accept-Charset", "Connection", "User-Agent"}
var config Config
var urlMap map[string]int

func ReadKafka(topic, group string, hosts []string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  hosts,
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 1000,
	})
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		request, err := ParseRequest(string(m.Value))
		u, err := url.Parse(request.Url)
		if err != nil {
			fmt.Println(err)
			continue
		}
		urlR := u.Host + u.Path
		_, ok := urlMap[urlR]
		if request.Method == "GET" && ok == false {
			urlMap[urlR] = 1
			DoGet(request, config.Network.Proxy)
		}
	}
}

func ParseRequest(msg string) (Request, error) {
	var request Request
	var err error
	var data map[string]interface{}
	if err = json.Unmarshal([]byte(msg), &data); err != nil {
		return request, err
	}
	request.Host = data["host"].(string)
	request.AgentId = data["agentId"].(string)
	request.Timestamp = int64(data["t"].(float64))
	request.Method = data["method"].(string)
	headers := make(map[string]string)
	for _, msg := range zeekMsg {
		//fmt.Println(msg)
		if data[msg].(string) != "-" {
			headers[msg] = data[msg].(string)
		}
	}
	port := data["resp_p"].(string)
	var schema string
	if port == "443" {
		schema = "https://"
	} else if port == "-" {
		return request, nil
	} else {
		schema = "http://"
	}
	request.Url = schema + headers["Host"] + data["uri"].(string)
	request.Headers = headers
	return request, err
}

func DoGet(request Request, proxy string) *resty.Response {
	client := resty.New()
	client.SetProxy(proxy)
	res := client.R()
	res.SetHeaders(request.Headers)
	fmt.Printf("Request to %s\n", request.Url)
	response, err := res.Get(request.Url)
	if err != nil {
		return nil
	}
	fmt.Println(response.StatusCode())
	return response
}

func init() {
	source, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		fmt.Println(err)
	}
	err = yaml.Unmarshal(source, &config)
	if err != nil {
		fmt.Println(err)
	}
	urlMap = make(map[string]int, 0)
}

func main() {
	ReadKafka(config.Kafka.Topic, config.Kafka.Group, config.Kafka.Brokers)
}
