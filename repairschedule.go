package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
)

func main () {
	login()
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
	log.Println(string(data))   // print whole html of user profile data
	return &client
}
