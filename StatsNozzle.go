package main

import (
	"crypto/tls"
	"fmt"
	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/cloudfoundry/noaa/consumer"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

/**
  if err != nill then print the given error, and if exitcode > 0 os.exit with that code
*/
func handleError(err error, exitcode int) {
	if err != nil {
		log.Print(err, "\n")
		if exitcode != 0 {
			os.Exit(exitcode)
		}
	}
}

type ConsoleDebugPrinter struct{}

func (c ConsoleDebugPrinter) Print(title, dump string) {
	println(title)
	println(dump)
}

var (
	apiAddress              = os.Getenv("API_ADDR")
	cfUsername              = os.Getenv("CF_USERNAME")
	cfPassword              = os.Getenv("CF_PASSWORD")
	tokenRefreshIntervalStr = os.Getenv("TOKEN_REFRESH_INTERVAL")
	apiClient               = cfclient.Client{}
	tokenRefreshInterval    int64
	eventTypes              = make(map[string]int)
	origins                 = make(map[string]int)
	jobs                    = make(map[string]int)
	deployments             = make(map[string]int)
	ips                     = make(map[string]int)
	lock                    = sync.RWMutex{}
)

func environmentComplete() bool {
	envComplete := true
	if apiAddress == "" {
		fmt.Println("missing envvar : API_ADDR")
		envComplete = false
	}
	if cfUsername == "" {
		fmt.Println("missing envvar : CF_USERNAME")
		envComplete = false
	}
	if cfPassword == "" {
		fmt.Println("missing envvar : CF_PASSWORD")
		envComplete = false
	}
	if len(tokenRefreshIntervalStr) == 0 {
		tokenRefreshInterval = 90
	} else {
		var err error
		tokenRefreshInterval, err = strconv.ParseInt(tokenRefreshIntervalStr, 0, 64)
		if err != nil {
			panic(err)
		}
	}
	log.SetOutput(os.Stdout)
	return envComplete
}

func getCFClient() *cfclient.Client {
	var err error
	c := &cfclient.Config{
		ApiAddress:        apiAddress,
		Username:          cfUsername,
		Password:          cfPassword,
		SkipSslValidation: true,
	}
	log.Print("getting cf client...")
	client, err := cfclient.NewClient(c)
	handleError(err, 8)

	// refresh the client every hour to get a new refresh token
	go func() {
		channel := time.Tick(time.Duration(tokenRefreshInterval) * time.Minute)
		for range channel {
			client, err := cfclient.NewClient(c)
			if err != nil {
				panic(err.Error())
			}
			log.Print("refreshed cfclient, got new token")
			apiClient = *client
		}
	}()

	return client
}

func main() {
	if !environmentComplete() {
		os.Exit(8)
	}

	// login to cf and get a client handle
	apiClient = *getCFClient()
	//
	// start sucking the firehose and handle events
	//
	cfInfo, err := apiClient.GetInfo()
	if err != nil {
		log.Fatal(err)
	}
	cons := consumer.New(cfInfo.DopplerLoggingEndpoint, &tls.Config{InsecureSkipVerify: true}, nil)
	cons.SetDebugPrinter(ConsoleDebugPrinter{})

	authToken, err := apiClient.GetToken()
	handleError(err, 8)
	firehoseChan, errorChan := cons.Firehose("StatsNozzle", authToken)

	go func() {
		for err := range errorChan {
			log.Printf("%v\n", err.Error())
		}
	}()

	go func() {
		for i := 0; i < 99999; i++ {
			lock.Lock()
			fmt.Print("\n\nEventTypes\n")
			for eventType := range eventTypes {
				fmt.Printf("  %s : %d\n", eventType, eventTypes[eventType])
			}
			fmt.Print("\nOrigins\n")
			for origin := range origins {
				fmt.Printf("  %s : %d\n", origin, origins[origin])
			}

			fmt.Print("\nJobs\n")
			for job := range jobs {
				fmt.Printf("  %s : %d\n", job, jobs[job])
			}

			fmt.Print("\nDeployments\n")
			for deployment := range deployments {
				fmt.Printf("  %s : %d\n", deployment, deployments[deployment])
			}

			fmt.Print("\nIPs\n")
			for ip := range ips {
				fmt.Printf("  %s : %d\n", ip, ips[ip])
			}

			lock.Unlock()
			time.Sleep(5 * time.Second)
		}
	}()

	for msg := range firehoseChan {
		lock.Lock()
		//log.Printf("EventType:%s Origin:%s Job:%s Deplymnt:%s Idx:%s IP:%s", msg.GetEventType(), msg.GetOrigin(), msg.GetJob(), msg.GetDeployment(), msg.GetIndex(), msg.GetIp())
		eventTypes[msg.GetEventType().String()] = eventTypes[msg.GetEventType().String()] + 1
		origins[msg.GetOrigin()] = origins[msg.GetOrigin()] + 1
		jobs[msg.GetJob()] = jobs[msg.GetJob()] + 1
		deployments[msg.GetDeployment()] = deployments[msg.GetDeployment()] + 1
		ips[msg.GetIp()] = ips[msg.GetIp()] + 1
		lock.Unlock()
	}
}
