package main

import (
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"github.com/go-redis/redis"
	"github.com/spf13/viper"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
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

var clientGetOpts = client.GetOptions{Recursive: true, Sort: true}

// ClientGet gets data from etcd sending in an url and receiving a etcd.Response object
func EtcdClientGet(kapi client.KeysAPI, url string) *client.Response {
	resp, err := kapi.Get(context.Background(), url, &clientGetOpts)
	if err != nil {
		Mlog.Print("Error on url", url, err.Error())
		os.Exit(2)
	}
	return resp
}

func parseRedisInfo(info string) map[string]string {
	lines := strings.Split(info, "\n")
	infoMap := make(map[string]string)
	for _, line := range lines {
		if strings.Index(line, ":") != -1 {
			kv := strings.Split(line, ":")
			Mlog.Print(line)
			infoMap[kv[0]] = strings.TrimSpace(kv[1])
		}
	}
	return infoMap
}

func processConnectedSlaves(s string) map[string]string {
	// ip=192.168.70.183,port=6379,state=online,offset=1317653926184,lag=0
	retMap := make(map[string]string, 0)
	kvpairs := strings.Split(s, ",")
	for _, kvpair := range kvpairs {
		kv := strings.Split(kvpair, "=")
		Mlog.Print(kv)
		retMap[kv[0]] = kv[1]
	}
	return retMap
}

func GetHostnameFromIP(s string) string {
	addr, err := net.LookupAddr(s)
	if err != nil {
		Mlog.Print("Lookup error on ", s, err.Error())
		return s
	}
	return addr[0]
}

type Redisinfo struct {
	Host      string
	Role      string
	Info      map[string]string
	SlaveList []string
}

func GetRedisInfo(hostlist []string) map[string]Redisinfo {
	redisInfoList := make(map[string]Redisinfo, 0)
	for _, redisHost := range hostlist {
		client := redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		info, err := client.Info("Replication").Result()
		if err != nil {
			Mlog.Print("Error on hoststring: ", redisHost, err)
		}
		hostMap := parseRedisInfo(info)
		hostMap["host"] = redisHost
		ri := Redisinfo{Host: redisHost, Info: hostMap, Role: hostMap["role"], SlaveList: nil}
		if hostMap["role"] == "master" {
			slaveList := make([]string, 0)
			if hostMap["connected_slaves"] != "0" {
				for key, value := range hostMap {
					if len(key) < 5 {
						continue
					}
					if key[:5] == "slave" {
						val := processConnectedSlaves(value)
						//slaveList = append(slaveList, val)
						// TODO -- attach it to the rest
						slaveList = append(slaveList, fmt.Sprintf("%s:%s", GetHostnameFromIP(val["ip"]), val["port"]))
					}
				}
				ri.SlaveList = slaveList
			}
		}
		s, _ := json.MarshalIndent(ri, "", "  ")
		//s, _ := json.Marshal(ri)
		Mlog.Print("struct:", string(s))
		redisInfoList[redisHost] = ri
	}
	return redisInfoList
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
		Mlog.Print("Etcd client error", err)
		return nil, err
	}
	return client.NewKeysAPI(c), nil
}

func GetServerList(kapi client.KeysAPI, url string) []string {
	resp := EtcdClientGet(kapi, url)
	hostList := make([]string, 0)
	// get the list of host type
	for _, n := range resp.Node.Nodes {
		// key format is /redis/cluster/site/<clusterkey>/<nodenum>
		hostName := strings.Join(strings.Split(n.Key, "/")[3:], "-")
		resp1 := EtcdClientGet(kapi, n.Key)
		for _, n1 := range resp1.Node.Nodes {
			// key format is /site/web/001 -- we want site-web-001
			bits := strings.Split(n1.Key, "/")
			port := bits[len(bits)-1]
			Mlog.Printf("full host: %s:%s\n", hostName, port)
			hostList = append(hostList, fmt.Sprintf("%s:%s", hostName, port))
		}
	}
	return hostList
}

func GetRedisMap(etcdHosts []string, clusterKeys []string) map[string]map[string]Redisinfo {
	kapi, err := GetEtcdKapi(etcdHosts)
	clusterMap := make(map[string]map[string]Redisinfo, 0)
	if err != nil {
		// we die on the inital because it assumes a user is there watching
		clusterMap["error"] = map[string]Redisinfo{fmt.Sprintf("Error getting etcdKAPI: %v", err.Error()): Redisinfo{}}
		return clusterMap
	}

	// spin through the clusterkeys
	for _, clusterKey := range clusterKeys {
		redisServerList := GetServerList(kapi, fmt.Sprintf("/redis/cluster/site/%s", clusterKey))
		Mlog.Print("serverlist: ", redisServerList)
		clusterMap[clusterKey] = GetRedisInfo(redisServerList)
	}
	return clusterMap
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

	clusterMap := GetRedisMap(viper.GetStringSlice("etcd_hosts"), viper.GetStringSlice("clusterkeys"))
	s, _ := json.MarshalIndent(clusterMap, "", "  ")
	Mlog.Print(string(s))
}
