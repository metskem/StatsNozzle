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
	apps                    = make(map[string]int)
	custom                  int
	lock                    = sync.RWMutex{}
	appinfoCache            = make(map[string]AppSpaceOrg)
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
			client, err = cfclient.NewClient(c)
			if err != nil {
				log.Printf("failed to refresh cfclient, error is %s", err)
			}
			log.Print("refreshed cfclient, got new token")
			apiClient = *client
		}
	}()

	return client
}

/*
  given an appguid, find out what the appname, spacename and orgname is
*/
func getAppInfoForGuid(appguid string) (AppSpaceOrg, error) {
	appInfo, ok := appinfoCache[appguid]
	if ok {
		return appInfo, nil
	}
	cfApp, err := apiClient.GetAppByGuidNoInlineCall(appguid)
	if err != nil {
		handleError(err, 0)
		return AppSpaceOrg{}, err
	}
	appName := cfApp.Name
	spaceName := cfApp.SpaceData.Entity.Name
	spaceGuid := cfApp.SpaceData.Entity.Guid
	orgName := cfApp.SpaceData.Entity.OrgData.Entity.Name
	orgGuid := cfApp.SpaceData.Entity.OrgData.Entity.Guid
	appinfoCache[appguid] = AppSpaceOrg{appName, appguid, spaceName, spaceGuid, orgName, orgGuid}
	return appinfoCache[appguid], nil
}

func printStats() {
	lock.Lock()
	defer lock.Unlock()
	fmt.Print("\n=================================================================================================\nEventTypes\n")
	vs := NewValSorter(eventTypes)
	vs.Sort()
	for eventType := range vs.Keys {
		fmt.Printf("  %s : %d\n", vs.Keys[eventType], vs.Vals[eventType])
	}
	fmt.Print("\nOrigins\n")
	vs = NewValSorter(origins)
	vs.Sort()
	for origin := range vs.Keys {
		fmt.Printf("  %s : %d\n", vs.Keys[origin], vs.Vals[origin])
	}

	fmt.Print("\nJobs\n")
	vs = NewValSorter(jobs)
	vs.Sort()
	for job := range vs.Keys {
		fmt.Printf("  %s : %d\n", vs.Keys[job], vs.Vals[job])
	}

	fmt.Print("\nDeployments\n")
	vs = NewValSorter(deployments)
	vs.Sort()
	for deployment := range vs.Keys {
		fmt.Printf("  %s : %d\n", vs.Keys[deployment], vs.Vals[deployment])
	}
	fmt.Print("\nIPs\n")
	vs = NewValSorter(ips)
	vs.Sort()
	for ip := range vs.Keys {
		fmt.Printf("  %s : %d\n", vs.Keys[ip], vs.Vals[ip])
	}

	//fmt.Print("\nApps\n")
	//vs = NewValSorter(apps)
	//vs.Sort()
	//for app := range vs.Keys {
	//	fmt.Printf("  %s : %d\n", vs.Keys[app], vs.Vals[app])
	//}

	fmt.Printf("\nCustom Value: %d \n", custom)
}

func main() {
	if !environmentComplete() {
		os.Exit(8)
	}

	log.SetPrefix("")
	logErr := log.New(os.Stderr, "", 0)
	logErr.SetOutput(os.Stderr)
	logErr.SetPrefix("")
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
			printStats()
			time.Sleep(5 * time.Second)
		}
	}()

	var appId string

	for msg := range firehoseChan {
		lock.Lock()

		logMessage := msg.GetLogMessage()
		if logMessage != nil {
			appId = logMessage.GetAppId()
			appSpaceOrg, err := getAppInfoForGuid(appId)
			if err != nil {
				fmt.Print(err)
			} else {
				key := appSpaceOrg.orgName + "/" + appSpaceOrg.spaceName + "/" + appSpaceOrg.appName
				apps[key] = apps[key] + 1
			}
		}

		//log.Printf("EventType:%s Origin:%s Job:%s Deplymnt:%s Idx:%s IP:%s", msg.GetEventType(), msg.GetOrigin(), msg.GetJob(), msg.GetDeployment(), msg.GetIndex(), msg.GetIp())
		eventTypes[msg.GetEventType().String()] = eventTypes[msg.GetEventType().String()] + 1
		origins[msg.GetOrigin()] = origins[msg.GetOrigin()] + 1
		jobs[msg.GetJob()] = jobs[msg.GetJob()] + 1
		deployments[msg.GetDeployment()] = deployments[msg.GetDeployment()] + 1
		ips[msg.GetIp()] = ips[msg.GetIp()] + 1
		lock.Unlock()
	}
}
