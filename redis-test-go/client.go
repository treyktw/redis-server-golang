package main

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	baseURL = "https://localhost:6379"
	apiKey  = "your-secret-api-key"
)

func makeRequest(method, url string, body string) (string, error) {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Note: In production, use proper certificate validation
		},
	}
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("X-API-Key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %v", err)
	}

	return string(respBody), nil
}

func main() {
	var resp string
	var err error

	// Test SET
	resp, err = makeRequest("POST", baseURL+"/set?key=mykey&value=Hello_World", "")
	fmt.Printf("SET response: %s, error: %v\n", resp, err)

	// Test expiration
	resp, err = makeRequest("POST", baseURL+"/expire?key=mykey&seconds=60", "")
	fmt.Printf("EXPIRE response: %s, error: %v\n", resp, err)

	// Get TTL
	resp, err = makeRequest("GET", baseURL+"/ttl/mykey", "")
	fmt.Printf("TTL response: %s, error: %v\n", resp, err)

	// Hash operations
	resp, err = makeRequest("POST", baseURL+"/hset?key=myhash&field=name&value=John", "")
	fmt.Printf("HSET response: %s, error: %v\n", resp, err)

	resp, err = makeRequest("GET", baseURL+"/hget/myhash/name", "")
	fmt.Printf("HGET response: %s, error: %v\n", resp, err)

	resp, err = makeRequest("GET", baseURL+"/hgetall/myhash", "")
	fmt.Printf("HGETALL response: %s, error: %v\n", resp, err)

	// List operations
	resp, err = makeRequest("POST", baseURL+"/lpush?key=mylist&value=first&value=second", "")
	fmt.Printf("LPUSH response: %s, error: %v\n", resp, err)

	resp, err = makeRequest("POST", baseURL+"/rpop/mylist", "")
	fmt.Printf("RPOP response: %s, error: %v\n", resp, err)

	// Set operations
	resp, err = makeRequest("POST", baseURL+"/sadd?key=myset&member=apple&member=banana", "")
	fmt.Printf("SADD response: %s, error: %v\n", resp, err)

	resp, err = makeRequest("GET", baseURL+"/smembers/myset", "")
	fmt.Printf("SMEMBERS response: %s, error: %v\n", resp, err)

	// Sorted set operations
	resp, err = makeRequest("POST", baseURL+"/zadd?key=myzset&score=1&member=one", "")
	fmt.Printf("ZADD response: %s, error: %v\n", resp, err)

	resp, err = makeRequest("GET", baseURL+"/zrange/myzset/0/-1", "")
	fmt.Printf("ZRANGE response: %s, error: %v\n", resp, err)

	resp, err = makeRequest("POST", baseURL+"/set?key=mykey&value=Hello_World", "")
	fmt.Printf("SET response: %s, error: %v\n", resp, err)

	// Test GET
	resp, err = makeRequest("GET", baseURL+"/get/mykey", "")
	fmt.Printf("GET response: %s, error: %v\n", resp, err)

	// Test EXPIRE
	resp, err = makeRequest("POST", baseURL+"/expire?key=mykey&seconds=60", "")
	fmt.Printf("EXPIRE response: %s, error: %v\n", resp, err)

	// Test TTL
	resp, err = makeRequest("GET", baseURL+"/ttl/mykey", "")
	fmt.Printf("TTL response: %s, error: %v\n", resp, err)

	// Test ZADD
	resp, err = makeRequest("POST", baseURL+"/zadd?key=myzset&score=1&member=one", "")
	fmt.Printf("ZADD response: %s, error: %v\n", resp, err)

	resp, err = makeRequest("POST", baseURL+"/zadd?key=myzset&score=2&member=two", "")
	fmt.Printf("ZADD response: %s, error: %v\n", resp, err)

	resp, err = makeRequest("POST", baseURL+"/zadd?key=myzset&score=3&member=three", "")
	fmt.Printf("ZADD response: %s, error: %v\n", resp, err)

	// Test ZRANGE
	resp, err = makeRequest("GET", baseURL+"/zrange/myzset/0/-1", "")
	fmt.Printf("ZRANGE response: %s, error: %v\n", resp, err)

	// Test ZREVRANGE
	resp, err = makeRequest("GET", baseURL+"/zrevrange/myzset/0/-1", "")
	fmt.Printf("ZREVRANGE response: %s, error: %v\n", resp, err)

	// Test ZREVRANGE
	resp, err = makeRequest("GET", baseURL+"/zrevrange/myzset/0/-1", "")
	fmt.Printf("ZREVRANGE response: %s, error: %v\n", resp, err)

	// Test ZRANK
	resp, err = makeRequest("GET", baseURL+"/zrank/myzset/two", "")
	fmt.Printf("ZRANK response: %s, error: %v\n", resp, err)

	// Test ZRANGEWITHSCORES
	resp, err = makeRequest("GET", baseURL+"/zrangewithscores/myzset/0/-1", "")
	fmt.Printf("ZRANGEWITHSCORES response: %s, error: %v\n", resp, err)

}
