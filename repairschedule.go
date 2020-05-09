package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
)

func main () {

	client := login()
	// add an individual schedule
	scheduleRepair(client)
}

func scheduleRepair(client *http.Client) {

	endpoint := "http://localhost:8080/repair_schedule"
	request, err := http.NewRequest("POST", endpoint, nil)
	request.Header.Set("Content-type", "application/json")
	// build required parameters for repairing a keyspace
	q := request.URL.Query()
	q.Add("clusterName", "premium1")
	q.Add("keyspace", "keyspace1")
	q.Add("owner", "lil' timmy")
	q.Add("scheduleTriggerTime", "2020-05-03T07:30:00")
	q.Add("scheduleDaysBetween", "10")
	// encode the params
	request.URL.RawQuery = q.Encode()
	// run the request
	response, err := client.Do(request)

	if err != nil {
		log.Println("❌ failed to post repair schedule")
		log.Fatal(err)
	}

	defer response.Body.Close()

	// no response is expected from reaper if the schedule addition is successful
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Println("❌ error reading request body")
		log.Fatal(err)
	}

	if string(body) == "" {
		log.Println("✅ successfully added repair schedule")
	} else {
		log.Fatal("❌ ", string(body))
	}
}

func login() *http.Client {

	jar, err := cookiejar.New(nil)
	if err != nil {
		log.Fatal(err)
	}
	client := http.Client{Jar: jar}
	resp, err := client.PostForm("http://localhost:8080/login", url.Values{
		"password": {"<todo>"},
		"username" : {"<todo>"},
	})
	if err != nil {
		log.Fatal(err)
	}

	data, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		log.Fatal(err)
	}
	log.Println(string(data))
	return &client
}
