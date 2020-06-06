package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type ScheduleEntry struct {
	ClusterName       string
	Keyspace          string
	Owner             string
	TriggerTime       time.Time
	DaysBetween       int
	Segments          int
	IncrementalRepair bool
	BlacklistedCFs    []string
	RepairThreadCount int
}

func main() {
	parseKSFile()
}

func scheduleRepair(client *http.Client, entry *ScheduleEntry) {

	endpoint := "http://localhost:8080/repair_schedule"
	request, _ := http.NewRequest("POST", endpoint, nil)
	request.Header.Set("Content-type", "application/json")
	// build required parameters for repairing a keyspace
	q := request.URL.Query()
	q.Add("clusterName", entry.ClusterName)
	q.Add("keyspace", entry.Keyspace)
	q.Add("owner", entry.Owner)
	q.Add("segmentCountPerNode", strconv.Itoa(entry.Segments))
	q.Add("scheduleTriggerTime", entry.TriggerTime.Format("2006-01-02T15:04:05"))
	q.Add("scheduleDaysBetween", strconv.Itoa(entry.DaysBetween))
	q.Add("repairParallelism", "PARALLEL")
	q.Add("incrementalRepair", strconv.FormatBool(entry.IncrementalRepair))
	q.Add("repairThreadCount", strconv.Itoa(entry.RepairThreadCount))
	// optional settings for larger repairs
	if entry.BlacklistedCFs != nil {
		jsonArr, _ := json.Marshal(entry.BlacklistedCFs)
		fmt.Println(fmt.Sprintf("%s", jsonArr))
		q.Add("blacklistedTables", fmt.Sprintf("%s", jsonArr))
	}

	// encode the params
	request.URL.RawQuery = q.Encode()
	// run the request
	response, err := client.Do(request)

	if err != nil {
		log.Println("❌ failed to post repair schedule: ", err)
	}

	defer response.Body.Close()

	// no response is expected from reaper if the schedule addition is successful
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Println("❌ error reading request body: ", err)
	}

	if string(body) == "" {
		log.Println("✅ successfully added repair schedule")
	} else {
		log.Println("❌ ", string(body))
	}
}

func login() *http.Client {

	jar, err := cookiejar.New(nil)
	if err != nil {
		log.Fatal(err)
	}
	client := http.Client{Jar: jar}
	resp, err := client.PostForm("http://localhost:8080/login", url.Values{
		//"password": {"<todo>"},
		//"username" : {"<todo>"},
		"password": {"admin"},
		"username": {"admin"},
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

func parseKSFile() {
	file, err := os.Open("/Users/lyubentodorov/Desktop/600keyspaces-may2020.csv")
	//file, err := os.Open("/Users/lyubentodorov/Desktop/3keyspaces-may2020.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	now := time.Now()
	// duration between repairs in the schedule in min
	// todo -- make this configurable
	repairInterval := 17
	fullRepairInterval := 360
	client := login()
	scanner := bufio.NewScanner(file)
	scheduleOffset := 1

	for scanner.Scan() {

		// CSV Expected format:
		// <keyspace>,<segmentCount>,<blacklistedTable1>-<blacklistedTable2>-<blacklistedTable..n>
		// eg with blacklisted tables: keyspace1,1000,standard1-standard2-standard3
		entryLine := strings.Split(scanner.Text(), ",")

		// default schedule
		entry := ScheduleEntry{
			// "premium1",
			"cassandra-sandbox1.prod.prod-ffs.io",
			entryLine[0],
			"owner",
			// add offset time between each schedule
			now.Add(time.Duration(scheduleOffset) * time.Minute),
			10,
			1,
			true,
			nil,
			1,
		}

		scheduleOffset += repairInterval

		if len(entryLine) > 1 {
			log.Println(entryLine)
			// number of splits for sub-range segmentation
			segments, err := strconv.Atoi(entryLine[1])
			if err != nil {
				log.Println("token conversion failed, expected an int, got ", entryLine[1])
			}

			entry.Segments = segments

			// todo -- csvs are not fit for purpose, use json / yaml instead.
			if len(entryLine) > 2 {
				// array of tables that should not be repaired
				entry.BlacklistedCFs = strings.Split(entryLine[2], "-")
			}

			entry.IncrementalRepair = false
			entry.TriggerTime = now.Add(time.Duration(scheduleOffset) * time.Minute)
			scheduleOffset += fullRepairInterval
		}

		// add an individual schedule
		scheduleRepair(client, &entry)
	}

	if err := scanner.Err(); err != nil {
		log.Println("❌ error reading csv: ", err)
	}
}
