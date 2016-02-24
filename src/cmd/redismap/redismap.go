package main

import (
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
	"os"
	"strings"
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
	if fileName != nil {
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalln("Failed to open log file", output, ":", err)
			os.Exit(1)
		}
		writer := io.MultiWriter(file, os.Stdout)
	} else {
		writer := io.Writer(os.Stdout)
	}
	Mlog := log.New(writer, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
}

var clientGetOpts = client.GetOptions{Recursive: true, Sort: true}

// ClientGet gets data from etcd sending in an url and receiving a etcd.Response object
func EtcdClientGet(kapi client.KeysAPI, url string) *client.Response {
	resp, err := kapi.Get(context.Background(), url, &clientGetOpts)
	if err != nil {
		Mlog.Print("Error on url", url, err.Error())
		os.Exit(2)
	}
	Mlog.Printf("%q key has %q value\n", resp.Node.Key, resp.Node.Value)
	return resp.Node.Value
}

func GetRedisInfo(string []hostlist) {
	for redisHost := range hostlist {
		client := redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		info, err := client.Info("Replication").Result()

		if err != nil {
			Mlog.Print("Error on hoststring: ", redisHost, err)
		}
		Mlog.Print("Info on: ", redisHost, info)
	}
}

func GetEtcdKapi(serverList []string) (client.KeysAPI, error) {
	cfg := client.Config{
		Endpoints: serverList,
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		logr.LogLine(logr.Lerror, ltagsrc, err.Error())
		return nil, err
	}
	return client.NewKeysAPI(c), nil
}

func main() {
	// we want to set up the series of keys that we'll want to retrieve that will
	// then contain the next set
	viper.SetConfigName("config")
	viper.AddConfigPath("$HOME/.redismap")
	viper.AddConfigPath("/etc/redismap")
	err := viper.ReadInConfig()

	if err != nil {
		fmt.Println("No configuration file loaded - aborting")
		os.Exit(1)
	}

	// init the logger
	logFileName := viper.GetStringSlice("logfile")
	Init(logFileName)

	clusterKeys := viper.GetStringSlice("clusterkeys")
	fmt.Printf("\n%s\n\n", clusterKeys)

	kapi, err := GetEtcdKapi(viper.GetStringSlice("etcd_hosts"))
	if err != nil {
		// we die on the inital because it assumes a user is there watching
		fmt.Printf("Error getting etcdKAPI\n\n", err.Error())
		os.Exit(2)
	}

	// spin through the clusterkeys
	for _, clusterKey := range clusterKeys {
		// do the etcd lookup to get list of redis hosts
		redisServerList := strings.Split(EtcdClientGet(clusterKey), ",")
		GetRedisInfo(redisServerList)
	}
}
