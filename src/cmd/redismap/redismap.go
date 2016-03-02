package main

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"github.com/tcotav/redismap"
	"io"
	"log"
	"os"
)

// retrieve clusterkey
// from etcd
// - get list of all hosts in cluster
// - verify each host in cluster
//	- get state of each host in cluster
//	- render list of masters with list of slaves, flagging no slave
//	- show any slave lag

var (
	Mlog *log.Logger
)

func Init(fileName string) {
	var writer io.Writer
	if fileName != "" {
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalln("Failed to open log file", fileName, ":", err)
			os.Exit(1)
		}
		writer = io.MultiWriter(file, os.Stdout)
	} else {
		writer = io.Writer(os.Stdout)
	}
	Mlog = log.New(writer, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	// we want to set up the series of keys that we'll want to retrieve that will
	// then contain the next set
	viper.SetConfigName("config")
	viper.AddConfigPath("$HOME/.redismap")
	//viper.AddConfigPath("/etc/redismap")
	err := viper.ReadInConfig()

	if err != nil {
		fmt.Println("No configuration file loaded - aborting", err.Error())
		os.Exit(1)
	}

	// init the logger
	logFileName := viper.GetStringSlice("logfile")
	Init(logFileName[0])
	//redismap.Init(logFileName[0])
	clusterMap := redismap.GetRedisMap(viper.GetStringSlice("etcd_hosts"), viper.GetStringSlice("clusterkeys"))
	s, _ := json.MarshalIndent(clusterMap, "", "  ")
	Mlog.Print(string(s))
}
