package main

import (
	"fmt"
	"time"
	"errors"
	"strings"
	"os/exec"
	"os"
	"strconv"
	"bytes"
	"path/filepath"
	"os/signal"
	"syscall"
	"io/ioutil"
	"regexp"

	"gopkg.in/alecthomas/kingpin.v2"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"github.com/nightlyone/lockfile"
	"github.com/parnurzeal/gorequest"
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

const (
	ver string = "0.48"
	dirDateLayout string = "2006-01-02_150405"
	logDateLayout string = "2006-01-02 15:04:05"
	lagWaitTimeout = 1440 // in deciseconds
	lagTolerance = 5 // in seconds
	defaultAlertmanagerSilenceDuration string = "12h"
	defaultAlertmanagerSilenceComment string = "mongo-backup"
)

var (
	stopBalancerTimeout = kingpin.Flag("timeout", "timeout for stopping mongo balancer in seconds").Default("1800").Short('t').Int()
	waitingChefStoppedTimeout = kingpin.Flag("chef-timeout", "waiting for chef stopped timeout in seconds").Default("900").Int()
	stopMongoDaemonDelay = kingpin.Flag("stop-delay", "delay after chef disabled to stop MongoDB daemon in seconds").Default("240").Int()
	mongosURL = kingpin.Flag("url", "mongos URL").Default("127.0.0.1:27017").Short('u').String()
	sshUser = kingpin.Flag("ssh-user", "ssh user").Default("backup_mongo").String()
	sshPort = kingpin.Flag("ssh-port", "ssh port").Default("22").Short('p').Int()
	mongoClusterName = kingpin.Flag("cluster-name", "MongoDB cluster name").Short('c').Required().String()
	mongoDataDir = kingpin.Flag("data-dir", "MongoDB data directory").Default("/var/lib/mongo/").String()
	dstRootPath = kingpin.Flag("dst-root-path", "destination root path").Default("/data/backup_mongo/files").String()
	retentionItems = kingpin.Flag("retention-items", "number of retention items to keep").Default("5").Int()
	verbose = kingpin.Flag("verbose", "verbose mode").Short('v').Bool()
	rsyncThreads = kingpin.Flag("rsync-threads", "number of concurrent rsync threads, default auto refers to number of mongo shards").Default("auto").String()
	rsyncRetries = kingpin.Flag("rsync-retries", "number of rsync retries").Default("2").Int()
	slackURL = kingpin.Flag("slack-url", "slack URL").Default("http://127.0.0.1").String()
	slackChannel = kingpin.Flag("slack-channel", "slack channel to send messages").Default("#it-automatic-logs").String()
	slackUsername = kingpin.Flag("slack-username", "slack username field").Default("mongo-backup").String()
	slackIconEmoji = kingpin.Flag("slack-icon-emoji", "slack icon-emoji field").Default(":mongo-backup:").String()
	pushgatewayURL = kingpin.Flag("pushgateway-url", "pushgateway URL").Default("").String()
	amtoolPath = kingpin.Flag("amtool", "path to amtool binary").Default("/usr/bin/amtool").String()
	alertmanagerURL = kingpin.Flag("alertmanager-url", "alertmanager URL").Default("").String()
)

var (
	MongoBackupSuccessTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mongo_backup_last_success_timestamp_seconds",
		Help: "The timestamp of the last successful completion of a mongo backup.",
	})
	MongoBackupSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mongo_backup_last_success",
		Help: "Success of the last mongo backup.",
	})
	MongoBackupDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mongo_backup_duration_seconds",
		Help: "The duration of the last mongo backup in seconds.",
	})
)

// Command : containts exec command data
type Command struct {
	cmd string
	args []string
}

// Lock : containts Lock data
type Lock struct {
	ID bson.ObjectId `bson:"_id,omitempty"`
	State int
}

// Msg : containts Msg data
type Msg struct {
	node string
	err error
}

// Job : containts Job data
type Job struct {
	clusterName string
	shardName string
	sshUser string
	sshPort int
	nodeURL string
	nodeMasterURL string
	srcPath string
	dstRootPath string
	dateString string
}

// Payload : containts slack Payload data
type Payload struct {
	Username string `json:"username"`
	Channel string `json:"channel"`
	IconEmoji string `json:"icon_emoji"`
	Attachments []Attachment `json:"attachments"`
}

// Attachment : containts slack Attachment data
type Attachment struct {
	Color string `json:"color"`
	Text string `json:"text"`
	MrkdwnIn []string `json:"mrkdwn_in"`
}

// Shard : containts Shard data
type Shard struct {
	Name string
	MasterURL string
	SelectedSlaveURL string
}

func stopBalancer(url string, stopBalancerTimeout int) error {
	session, err := mgo.Dial(url)
	if err != nil {
		return err
	}
	defer session.Close()

	log.Info("Disabling mongo balancer")
	c := session.DB("config").C("settings")
	err = c.Update(bson.M{"_id": "balancer"}, bson.M{"$set": bson.M{"stopped": true}})
	if err != nil {
		return fmt.Errorf("Cannot disable balancer, if balancer was never disabled on this cluster try enabling and disabling it manually, sh.stopBalancer(), sh.startBalancer(): %v", err)
	}

	log.Info("Waiting for balancer unlocked")
	c = session.DB("config").C("locks")
	result := Lock{}
	unlocked := false
	for i := 0; i < stopBalancerTimeout; i++ {
		err := c.Find(bson.M{"_id": "balancer"}).One(&result)
		if err == nil {
			if result.State == 0 {
				unlocked = true
				break
			}
		} else {
			log.Errorf("Mongo query failed: %s", err)
		}

		log.Debugf("Waiting for balancer unlocked: %d seconds", i)
		time.Sleep(time.Second)
	}

	if !unlocked {
		return errors.New("Balancer is still locked after timeout")
	}

	return nil
}

func startBalancer(url string) error {
	session, err := mgo.Dial(url)
	if err != nil {
		return err
	}
	defer session.Close()

	c := session.DB("config").C("settings")

	log.Info("Enabling mongo balancer")
	err = c.Update(bson.M{"_id": "balancer"}, bson.M{"$set": bson.M{"stopped": false}})

	return err
}

func getShardMaster(url string) (string, error) {
	session, err := mgo.Dial(url)
	if err != nil {
		return "", err
	}
	defer session.Close()

	result := bson.M{}
	err = session.DB("admin").Run("isMaster", &result)
	if err != nil {
		return "", err
	}

	if _, ok := result["primary"]; !ok {
		return "", fmt.Errorf("Cannot find 'primary' field in received data from %s", url)
	}

	return result["primary"].(string), nil
}

func getRSStatus(url string) (map[string]interface{}, error) {
	session, err := mgo.Dial(url)
	if err != nil {
		return map[string]interface{}{}, err
	}
	defer session.Close()

	result := make(map[string]interface{})
	err = session.DB("admin").Run("replSetGetStatus", &result)
	if err != nil {
		return map[string]interface{}{}, err
	}

	if _, ok := result["members"]; !ok {
		return map[string]interface{}{}, fmt.Errorf("Cannot find 'members' field in received data from %s", url)
	}

	return result, nil
}

func extractMemberDate(status map[string]interface{}, masterURL, memberURL string) (uint64, error) {
	found := false
	for _, member := range status["members"].([]interface{}) {
		if member.(map[string]interface{})["name"] == memberURL {
			found = true

			if value, ok := member.(map[string]interface{})["optimeDate"]; ok {
				return uint64(value.(time.Time).Unix()), nil
			} else {
				return 0, fmt.Errorf("%s: cannot find member %s field 'optimeDate' in replica set status", stripPort(masterURL), memberURL)
			}
		}
	}

	if !found {
		return 0, fmt.Errorf("%s: cannot find member %s in replica set status", stripPort(masterURL), memberURL)
	}

	return 0, nil
}

func isSecondaryLagged(masterURL, memberURL string) (bool, error) {
	status, err := getRSStatus(masterURL)
	if err != nil {
		return false, err
	}

	timestampMaster, err := extractMemberDate(status, masterURL, masterURL)
	if err != nil {
		return false, err
	}

	timestampMember, err := extractMemberDate(status, masterURL, memberURL)
	if err != nil {
		return false, err
	}

	if timestampMaster - timestampMember > lagTolerance {
		log.Debugf("%s: secondary member %s lag: %d", stripPort(masterURL), memberURL, timestampMaster - timestampMember)
		return true, nil
	}

	return false, nil
}

func waitSecondaryNotLagged(masterURL, memberURL string) error {
	log.Infof("%s: waiting member %s is not lagged", stripPort(masterURL), stripPort(memberURL))
	success := false

	for i := 0; i < lagWaitTimeout; i++ {
		lag, err := isSecondaryLagged(masterURL, memberURL)
		if err != nil {
			return err
		}

		if !lag {
			success = true
			break
		}
		time.Sleep(time.Second * 10)
	}

	if success {
		log.Infof("%s: member %s is not lagged", stripPort(masterURL), stripPort(memberURL))
	} else {
		return fmt.Errorf("%s: waiting for secondary member %s not lagged timeout", stripPort(masterURL), stripPort(memberURL))
	}

	return nil
}

func getRSConfig(url string) (map[string]interface{}, error) {
	session, err := mgo.Dial(url)
	if err != nil {
		return map[string]interface{}{}, err
	}
	defer session.Close()

	result := make(map[string]interface{})
	err = session.DB("admin").Run("replSetGetConfig", &result)
	if err != nil {
		return map[string]interface{}{}, err
	}

	if _, ok := result["config"]; !ok {
		return map[string]interface{}{}, fmt.Errorf("Cannot find 'config' field in received data from %s", url)
	}

	return result, nil
}

func setRSConfig(url string, config map[string]interface{}) error {
	session, err := mgo.Dial(url)
	if err != nil {
		return err
	}
	defer session.Close()

	log.Infof("%s: applying new replica set config", stripPort(url))
	err = session.Run(bson.D{{"replSetReconfig", config}}, nil)
	if err != nil {
		return err
	}

	return nil
}

func hideMembers(shards []Shard) error {
	result := make(chan error, len(shards))

	for _, shard := range shards {
		go hideMember(shard.MasterURL, shard.SelectedSlaveURL, result)
	}

	if !allResultsSuccess(len(shards), result) {
		return errors.New("Hidding members failed on one or more nodes")
	}

	return nil
}

func hideMember(url, targetMember string, result chan<- error) {
	config, err := getRSConfig(url)
	if err != nil {
		result <- err
		return
	}

	log.Infof("%s: creating new replica set config with hidden %s member", stripPort(url), targetMember)

	success := false
	for _, member := range config["config"].(map[string]interface{})["members"].([]interface{}) {
		if member.(map[string]interface{})["host"] == targetMember {
			member.(map[string]interface{})["priority"] = 0
			member.(map[string]interface{})["hidden"] = true
			success = true
			break
		}
	}

	if !success {
		result <- fmt.Errorf("Cannot find member %s in current replica set config", targetMember)
		return
	}

	config["config"].(map[string]interface{})["version"] = config["config"].(map[string]interface{})["version"].(int) + 1


	if err := setRSConfig(url, config["config"].(map[string]interface{})); err != nil {
		result <- err
		return
	}

	result <- nil
}

func unhideMembers(shards []Shard) error {
	result := make(chan Msg, len(shards))

	for _, shard := range shards {
		go waitAndUnhideMember(shard.MasterURL, shard.SelectedSlaveURL, result)
	}

	if !allResultsSuccessM(len(shards), result) {
		return errors.New("Unhidding members failed on one or more nodes")
	}

	return nil
}

func unhideMember(url, targetMember string, result chan<- error) {
	config, err := getRSConfig(url)
	if err != nil {
		result <- err
		return
	}

	log.Infof("%s: creating new replica set config with unhidden %s member", stripPort(url), targetMember)

	success := false
	for _, member := range config["config"].(map[string]interface{})["members"].([]interface{}) {
		if member.(map[string]interface{})["host"] == targetMember {
			member.(map[string]interface{})["priority"] = 1
			member.(map[string]interface{})["hidden"] = false
			success = true
			break
		}
	}

	if !success {
		result <- fmt.Errorf("Cannot find member %s in current replica set config", targetMember)
		return
	}

	config["config"].(map[string]interface{})["version"] = config["config"].(map[string]interface{})["version"].(int) + 1


	if err := setRSConfig(url, config["config"].(map[string]interface{})); err != nil {
		result <- err
		return
	}

	result <- nil
}

func unhideSingleMember(masterURL, node string) error {
	result := make(chan error)

	go unhideMember(masterURL, node, result)

	if err := <-result; err != nil {
		return fmt.Errorf("%s: cannot unhide member: %v", node, err)
	}

	return nil
}

func waitAndUnhideMember(masterURL, memberURL string, results chan<- Msg) {
	var msg Msg
	msg.node = stripPort(memberURL)

	if err := waitSecondaryNotLagged(masterURL, memberURL); err != nil {
		msg.err = err
		results <- msg
		return
	}

	if err := unhideSingleMember(masterURL, memberURL); err != nil {
		msg.err = err
		results <- msg
		return
	}

	results <- msg
}

func dbStats(url string) (bson.M, error) {
	session, err := mgo.Dial(url)
	if err != nil {
		return bson.M{}, err
	}
	defer session.Close()

	result := bson.M{}
	err = session.DB("test").Run("dbstats", &result)
	if err != nil {
		return bson.M{}, err
	}

	if _, ok := result["raw"]; !ok {
		return bson.M{}, fmt.Errorf("Cannot find 'raw' field in received data from %s", url)
	}

	return result["raw"].(bson.M), nil
}

func getShardsData(url string) (map[string][]map[string]string, error) {
	shards := make(map[string][]map[string]string)

	data, err := dbStats(url)
	if err != nil {
		return map[string][]map[string]string{}, err
	}

	for k := range data {
		shardStr := strings.Split(k, "/")
		shardName := shardStr[0]

		shardNodes := strings.Split(shardStr[1], ",")
		for _, node := range shardNodes {
			shards[shardName] = append(shards[shardName], map[string]string{"node_url": node})
		}
	}

	return shards, nil
}

func markMasterShard(url string) (map[string][]map[string]string, error) {
	shards, err := getShardsData(url)
	if err != nil {
		return map[string][]map[string]string{}, err
	}

	markedShards := make(map[string][]map[string]string)
	for shard, nodes := range shards {
		var master string
		for _, node := range nodes {
			master, err = getShardMaster(node["node_url"])
			if err != nil {
				log.Warn(err)
			} else {
				break
			}
		}

		if master != "" {
			markedShards[shard] = nodes
			for _, node := range markedShards[shard] {
				if node["node_url"] == master {
					node["master"] = ""
				}
			}
		} else {
			log.Warnf("Cannot find master node in shard %s", shard)
		}
	}

	log.Infof("Found shards: %v", markedShards)

	return markedShards, nil
}

func getShards(url string) ([]Shard, error) {
	shardsData, err := markMasterShard(url)
	if err != nil {
		return []Shard{}, err
	}

	var shards []Shard
	for shardName, nodes := range shardsData {
		var shard Shard
		shard.Name = shardName

		for _, node := range nodes {
			if _, ok := node["master"]; ok {
				shard.MasterURL = node["node_url"]
				break
			}
		}

		for _, node := range nodes {
			if _, ok := node["master"]; !ok {
				shard.SelectedSlaveURL = node["node_url"]
				break
			}
		}

		if shard.MasterURL == "" || shard.SelectedSlaveURL == "" {
			log.Warnf("No node data for shard %s", shardName)
		}
		shards = append(shards, shard)

	}

	return shards, nil
}

func stripPort(url string) string {
	return strings.Split(url, ":")[0]
}

func prepareSSHCommands(nodeName, sshUser string, sshPort int, remoteCmd []string) Command {
	var command Command
	command.cmd = "ssh"
	command.args = []string{
		"-o",
		"StrictHostKeyChecking=no",
		"-o",
		"PasswordAuthentication=no",
		"-p",
		strconv.Itoa(sshPort),
		"-l",
		sshUser,
		nodeName,
	}
	command.args = append(command.args, remoteCmd...)

	return command
}

func disableChef(nodeName, sshUser string, sshPort, waitingChefStoppedTimeout int, result chan<- error) {
	log.Infof("%s: disabling chef-client (/etc/disabled/chef)", nodeName)

	if err := executeCommand(prepareSSHCommands(nodeName, sshUser, sshPort, []string{"sudo", "touch", "/etc/disabled/chef"})); err == nil {
		if err := waitingChefStopped(nodeName, sshUser, sshPort, waitingChefStoppedTimeout); err != nil {
			result <- err
			return
		}
	} else {
		result <- err
		return
	}

	result <- nil
}

func disableMongo(nodeName, sshUser string, sshPort int, result chan<- error) {
	log.Infof("%s: disabling mongod (/etc/disabled/mongo)", nodeName)

	result <- executeCommand(prepareSSHCommands(nodeName, sshUser, sshPort, []string{"sudo", "touch", "/etc/disabled/mongo"}))
}

func stopMongoDaemon(nodeName, sshUser string, sshPort int, result chan<- error) {
	log.Infof("%s: stopping mongod daemon", nodeName)

	if err := executeCommand(prepareSSHCommands(nodeName, sshUser, sshPort, []string{"sudo", "systemctl", "stop", "mongod.service"})); err != nil {
		result <- err
		return
	}

	log.Infof("%s: mongod daemon stopped", nodeName)
	flushBuffers(nodeName, sshUser, sshPort)

	result <- nil
}

func startMongoDaemon(nodeName, sshUser string, sshPort int) error {
	log.Infof("%s: starting mongod daemon", nodeName)

	return executeCommand(prepareSSHCommands(nodeName, sshUser, sshPort, []string{"sudo", "systemctl", "start", "mongod.service"}))
}

func enableChef(nodeName, sshUser string, sshPort int) error {
	log.Infof("%s: enabling chef-client (/etc/disabled/chef)", nodeName)

	return executeCommand(prepareSSHCommands(nodeName, sshUser, sshPort, []string{"sudo", "rm", "-f", "/etc/disabled/chef"}))
}

func enableMongo(nodeName, sshUser string, sshPort int) error {
	log.Infof("%s: enabling mongod (/etc/disabled/mongo)", nodeName)

	return executeCommand(prepareSSHCommands(nodeName, sshUser, sshPort, []string{"sudo", "rm", "-f", "/etc/disabled/mongo"}))
}

func flushBuffers(nodeName, sshUser string, sshPort int) error {
	log.Infof("%s: flushing disk buffers", nodeName)

	return executeCommand(prepareSSHCommands(nodeName, sshUser, sshPort, []string{"sync"}))
}

func waitingChefStopped(nodeName, sshUser string, port, waitingChefStoppedTimeout int) error {
	log.Infof("%s: waiting chef-client not running", nodeName)

	success := false
	for i := 0; i < waitingChefStoppedTimeout; i++ {
		if i > 0 {
			log.Debugf("%s: waiting for chef-client stopped", nodeName)
			time.Sleep(time.Second * 1)
		}
		if err := executeCommand(prepareSSHCommands(nodeName, sshUser, port, []string{"sudo", "pgrep", "chef-client"})); err == nil {
			continue
		}

		if err := executeCommand(prepareSSHCommands(nodeName, sshUser, port, []string{"sudo", "pgrep", "lazy_chef_run"})); err == nil {
			continue
		}

		success = true
		break
	}

	if !success {
		return fmt.Errorf("%s: waiting chef-client stopped timeout", nodeName)
	}

	return nil
}

func silenceAlertmanagerAlert(instance, amtoolPath, alertmanagerURL string) error {
	instanceShort := strings.Split(instance, ".")[0]

	var command Command
	command.cmd = amtoolPath
	command.args = []string{
		"--alertmanager.url=" + alertmanagerURL,
		"silence",
		"add",
		"-c",
		defaultAlertmanagerSilenceComment,
		"-d",
		defaultAlertmanagerSilenceDuration,
		"instance=" + instanceShort,
	}
	if err := executeCommand(command); err != nil {
		return err
	}

	return nil
}

func removeAlertmanagerSilence(instance, amtoolPath, alertmanagerURL string) error {
	instanceShort := strings.Split(instance, ".")[0]

	var command Command
	command.cmd = "/bin/sh"
	command.args = []string{
		"-c",
		amtoolPath + " --alertmanager.url=" + alertmanagerURL + " silence expire $(" + amtoolPath + " --alertmanager.url=" + alertmanagerURL + " silence query -q instance=" + instanceShort + ")",
	}
	if err := executeCommand(command); err != nil {
		return err
	}

	return nil
}

func silenceAlertmanagerAlerts(shards []Shard, amtoolPath, alertmanagerURL string) error {
	var errors []string
	if alertmanagerURL != "" {
		for _, shard := range shards {
			if err := silenceAlertmanagerAlert(stripPort(shard.SelectedSlaveURL), amtoolPath, alertmanagerURL); err != nil {
				errors = append(errors, fmt.Sprintf("%s", err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "; "))
	}

	return nil
}

func removeAlertmanagerSilences(shards []Shard, amtoolPath, alertmanagerURL string) error {
	var errors []string
	if alertmanagerURL != "" {
		for _, shard := range shards {
			if err := removeAlertmanagerSilence(stripPort(shard.SelectedSlaveURL), amtoolPath, alertmanagerURL); err != nil {
				errors = append(errors, fmt.Sprintf("%s", err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "; "))
	}

	return nil
}

func drainNodes(shards []Shard, sshUser string, sshPort, waitingChefStoppedTimeout, stopMongoDaemonDelay int, amtoolPath, alertmanagerURL string) error {
	failed := false
	if err := disableChefOnNodes(shards, sshUser, sshPort, waitingChefStoppedTimeout); err != nil {
		log.Error(err)
		failed = true
	}

	if !failed {
		if err := silenceAlertmanagerAlerts(shards, amtoolPath, alertmanagerURL); err != nil {
			log.Error(err)
		}
	}

	if !failed {
		if err := disableMongoOnNodes(shards, sshUser, sshPort); err != nil {
			log.Error(err)
			failed = true
		}
	}

	if !failed {
		log.Infof("Waiting delay %d seconds", stopMongoDaemonDelay)
		time.Sleep(time.Second * time.Duration(stopMongoDaemonDelay))

		if err := stopMongoDaemonOnNodes(shards, sshUser, sshPort); err != nil {
			log.Error(err)
			failed = true
		}
	}

	if failed {
		return errors.New("Draining failed on one or more nodes")
	}

	return nil
}

func disableChefOnNodes(shards []Shard, sshUser string, sshPort, waitingChefStoppedTimeout int) error {
	result := make(chan error, len(shards))

	for _, shard := range shards {
		go disableChef(stripPort(shard.SelectedSlaveURL), sshUser, sshPort, waitingChefStoppedTimeout, result)
	}

	if !allResultsSuccess(len(shards), result) {
		return errors.New("Disabling chef failed on one or more nodes")
	}

	return nil
}

func disableMongoOnNodes(shards []Shard, sshUser string, sshPort int) error {
	result := make(chan error, len(shards))

	for _, shard := range shards {
		go disableMongo(stripPort(shard.SelectedSlaveURL), sshUser, sshPort, result)
	}

	if !allResultsSuccess(len(shards), result) {
		return errors.New("Disabling mongod failed on one or more nodes")
	}

	return nil
}

func stopMongoDaemonOnNodes(shards []Shard, sshUser string, sshPort int) error {
	result := make(chan error, len(shards))

	for _, shard := range shards {
		go stopMongoDaemon(stripPort(shard.SelectedSlaveURL), sshUser, sshPort, result)
	}

	if !allResultsSuccess(len(shards), result) {
		return errors.New("Stopping mongod daemon failed on one or more nodes")
	}

	return nil
}

func activateNode(nodeName, sshUser string, sshPort int, result chan<- error) {
	var errors []string
	if err := enableChef(nodeName, sshUser, sshPort); err != nil {
		errors = append(errors, fmt.Sprintf("%s: cannot enable chef-client", nodeName))
	}

	if err := enableMongo(nodeName, sshUser, sshPort); err != nil {
		errors = append(errors, fmt.Sprintf("%s: cannot enable mongod", nodeName))
	}

	if err := startMongoDaemon(nodeName, sshUser, sshPort); err != nil {
		errors = append(errors, fmt.Sprintf("%s: cannot start mongod daemon", nodeName))
	}

	if len(errors) > 0 {
		result <- fmt.Errorf("%s", strings.Join(errors, "; "))
		return
	}

	result <- nil
}

func activateSingleNode(nodeName, sshUser string, sshPort int) {
	result := make(chan error)

	go activateNode(nodeName, sshUser, sshPort, result)

	if err := <-result; err != nil {
		log.Errorf("%s: cannot enable mongo node: %v", nodeName, err)
	}
}

func activateNodes(shards []Shard, sshUser string, sshPort int) error {
	result := make(chan error, len(shards))

	for _, shard := range shards {
		go activateNode(stripPort(shard.SelectedSlaveURL), sshUser, sshPort, result)
	}

	if !allResultsSuccess(len(shards), result) {
		return errors.New("One or more nodes activate failed")
	}

	return nil
}

func allResultsSuccess(numOfWorkers int, result <-chan error) bool {
	success := true
	for i := 0; i < numOfWorkers; i++ {
		if err := <-result; err != nil {
			log.Error(err)
			success = false
		}
	}

	return success
}

func cleanup(url string, shards []Shard, sshUser string, sshPort int, amtoolPath, alertmanagerURL string) {
	if err := activateNodes(shards, sshUser, sshPort); err != nil {
		log.Error(err)
	}

	if err := startBalancer(url); err != nil {
		log.Error(err)
	}

	if err := unhideMembers(shards); err != nil {
		log.Error(err)
	}

	if err := removeAlertmanagerSilences(shards, amtoolPath, alertmanagerURL); err != nil {
		log.Error(err)
	}
}

func addTrailingSlashIfNotExists(path string) string {
	char := path[len(path)-1]
	if string(char) != "/" {
		path = path + "/"
	}

	return path
}

func deleteTrailingSlashIfExists(path string) string {
	char := path[len(path)-1]
	if string(char) == "/" {
		path = path[:len(path)-1]
	}

	return path
}

func retentionDataCleanup(rootPath string, retentionItems int) error {
	files, err := ioutil.ReadDir(rootPath)
	if err != nil {
		return err
	}

	for i := len(files)-1; i >= 0; i-- {
		if retentionItems <= 0 {
			if match, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}_\d{6}$`, files[i].Name()); match {
				path := rootPath + "/" + files[i].Name()

				log.Infof("Deleting directory %s", path)
				os.RemoveAll(path)
			}
		}
		retentionItems--
	}

	return nil
}

func processNodes(
		mongoClusterName,
		url string,
		stopBalancerTimeout int,
		sshUser string,
		sshPort,
		waitingChefStoppedTimeout int,
		mongoDataDir,
		dstRootPath string,
		retentionItems,
		stopMongoDaemonDelay int,
		rsyncThreadsFromParameter string,
		rsyncRetries int,
		amtoolPath string,
		alertmanagerURL string,
		lock lockfile.Lockfile) error {
	shards, err := getShards(url)
	if err != nil {
		return err
	}

	var rsyncThreads int
	if rsyncThreadsFromParameter == "auto" {
		rsyncThreads = len(shards)
	} else {
		rsyncThreads, err = strconv.Atoi(rsyncThreadsFromParameter)
		if err != nil {
			return fmt.Errorf("Cannot parse rsync-threads parameter: %v", err)
		}
	}

	mongoDataDir = addTrailingSlashIfNotExists(mongoDataDir)
	dstRootPath = deleteTrailingSlashIfExists(dstRootPath)
	dateString := fmt.Sprint(time.Now().Format(dirDateLayout))

	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		<-sigchan
		log.Error("Program killed!")

		cleanup(url, shards, sshUser, sshPort, amtoolPath, alertmanagerURL)

		lock.Unlock()
		os.Exit(1)
	}()

	if err := stopBalancer(url, stopBalancerTimeout); err != nil {
		return err
	}

	if err := hideMembers(shards); err != nil {
		return err
	}

	var processErr error
	if err := drainNodes(shards, sshUser, sshPort, waitingChefStoppedTimeout, stopMongoDaemonDelay, amtoolPath, alertmanagerURL); err == nil {
		if err := rsyncBackups(shards, mongoClusterName, sshUser, sshPort, mongoDataDir, dstRootPath, dateString, rsyncThreads, rsyncRetries); err != nil {
			log.Error(err)
			processErr = err
		}
	// if draining nodes fail
	} else {
		log.Error(err)
		processErr = err

		if err := activateNodes(shards, sshUser, sshPort); err != nil {
			log.Error(err)
		}

		if err := unhideMembers(shards); err != nil {
			log.Error(err)
		}
	}

	if err := startBalancer(url); err != nil {
		return err
	}

	if err := removeAlertmanagerSilences(shards, amtoolPath, alertmanagerURL); err != nil {
		log.Error(err)
	}

	if processErr == nil {
		retentionDataCleanup(dstRootPath + "/" + mongoClusterName, retentionItems)
	} else {
		return processErr
	}

	return nil
}

func allResultsSuccessM(numOfWorkers int, channel <-chan Msg) bool {
	success := true

	for i := 0; i < numOfWorkers; i++ {
		msg := <-channel
		if msg.err != nil {
			log.Error(msg.err)
			success = false
		}
	}

	return success
}

func rsyncBackups(shards []Shard, clusterName, sshUser string, sshPort int, srcPath, dstRootPath, dateString string, rsyncThreads, rsyncRetries int) error {
	jobs := make(chan Job, len(shards))
	results := make(chan Msg, len(shards))
	unhideResults := make(chan Msg, len(shards))

	for w := 1; w <= rsyncThreads; w++ {
		go rsyncWorker(rsyncRetries, jobs, results, unhideResults)
	}

	for _, shard := range shards {
		var j Job
		j.clusterName = clusterName
		j.sshUser = sshUser
		j.sshPort = sshPort
		j.srcPath = srcPath
		j.dstRootPath = dstRootPath
		j.shardName = shard.Name
		j.nodeURL = shard.SelectedSlaveURL
		j.nodeMasterURL = shard.MasterURL
		j.dateString = dateString

		jobs <- j
	}
	close(jobs)

	var errs []string

	if !allResultsSuccessM(len(shards), results) {
		errs = append(errs, "One or more rsync failed")
	}

	if !allResultsSuccessM(len(shards), unhideResults) {
		errs = append(errs, "Unhide one or more node failed")
	}

	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "; "))
	}

	return nil
}

func rsyncExecute(rsyncRetries int, clusterName, shardName, sshUser string, sshPort int, nodeName, srcPath, dstPath string) error {
	success := false
	for i := 0; i < rsyncRetries; i++ {
		log.Infof("Shard %s: performing rsync backup from %s", shardName, nodeName)
		if err := executeCommand(prepareRsyncCommands(clusterName, shardName, sshUser, sshPort, nodeName, srcPath, dstPath)); err == nil {
			success = true
			break
		} else {
			log.Errorf("Shard %s: rsync failed from node %s: %v", shardName, nodeName, err)
		}
	}

	if !success {
		return fmt.Errorf("Shard %s: rsync failed from node %s", shardName, nodeName)
	}

	log.Infof("Shard %s: rsync from %s completed successfully", shardName, nodeName)

	return nil
}

func rsyncWorker(rsyncRetries int, jobs <-chan Job, results chan<- Msg, unhideResults chan<- Msg) {
	for j := range jobs {
		nodeName := stripPort(j.nodeURL)

		var msg Msg
		msg.node = nodeName

		dstPath := j.dstRootPath + "/" + j.clusterName + "/" + j.dateString + "/" + j.shardName

		log.Infof("Creating directory %s", dstPath)
		var command Command
		command.cmd = "mkdir"
		command.args = []string{
			"-p",
			dstPath,
		}
		if err := executeCommand(command); err != nil {
			msg.err = err
		}

		if msg.err == nil {
			if err := rsyncExecute(rsyncRetries, j.clusterName, j.shardName, j.sshUser, j.sshPort, nodeName, j.srcPath, dstPath); err != nil {
				msg.err = err
			}
		}

		activateSingleNode(nodeName, j.sshUser, j.sshPort)

		go waitAndUnhideMember(j.nodeMasterURL, j.nodeURL, unhideResults)

		if msg.err != nil {
			// when one job failed take all jobs to avoid processing another
			for f := range jobs {
				var m Msg
				m.node = stripPort(f.nodeURL)
				m.err = errors.New("Creating new job aborted")
				results <- m
			}
			results <- msg
			continue
		}

		msg.err = nil
		results <- msg
	}
}

func prepareRsyncCommands(clusterName, shardName, sshUser string, sshPort int, nodeName, srcPath, dstPath string) Command {
	var command Command
	command.cmd = "rsync"
	command.args = []string{
		"-avHAX",
		"--delete",
		"--inplace",
		"-e",
		fmt.Sprintf("ssh -o StrictHostKeyChecking=no -p %d", sshPort),
		sshUser + "@" + nodeName + ":" + srcPath,
		dstPath,
	}

	return command
}

func executeCommand(command Command) error {
	cmd := exec.Command(command.cmd, command.args...)
	log.Debugf("Executing command: %v %v", command.cmd, strings.Join(command.args, " "))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	if err != nil {
		return fmt.Errorf("%s: %s", err, stderr.String())
	}

	return nil
}

func sendSlackMsg(webhookURL, channel, cluster, username, color, iconEmoji string, timeout int) error {
	payload := Payload{
		Username: username,
		Channel: channel,
		IconEmoji: iconEmoji,
		Attachments: []Attachment{
			Attachment{
				Color: color,
				Text: fmt.Sprintf("Cluster *%s* backup failed", cluster),
				MrkdwnIn: []string{"text"},
			},
		},
	}

	json, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	result := make(chan error)
	go httpPost(webhookURL, string(json), result)

	select {
	case err := <-result:
		return err
	case <-time.After(time.Second * time.Duration(timeout)):
		return errors.New("Slack connections timeout")
	}
}

func httpPost(url, data string, result chan<- error) {
	request := gorequest.New()
	resp, _, errs := request.Post(url).
		Send(data).
		End()

	if errs != nil {
		var errsStr []string
		for _, e := range errs {
			errsStr = append(errsStr, fmt.Sprintf("%s", e))
		}
		result <- fmt.Errorf("%s", strings.Join(errsStr, ", "))
		return
	}

	if resp.StatusCode != 200 {
		result <- fmt.Errorf("HTTP response code: %s", resp.Status)
		return
	}

	result <- nil
}

func getInstance() (string, string) {
	var k, v string
	for k, v = range push.HostnameGroupingKey() {}
	return k, v
}

func pushgatewayInitialize(pushgatewayURL, jobName, clusterName string) (*push.Pusher, time.Time) {
	if pushgatewayURL != "" {
		registry := prometheus.NewRegistry()
		registry.MustRegister(MongoBackupSuccessTime, MongoBackupSuccess, MongoBackupDuration)
		pusher := push.New(pushgatewayURL, jobName).Grouping(getInstance()).Grouping("cluster_name", clusterName).Gatherer(registry)

		return pusher, time.Now()
	}

	return &push.Pusher{}, time.Time{}
}

func sendPushgatewayMetrics(success bool, pushgatewayURL string, start time.Time, pusher *push.Pusher) {
	if pushgatewayURL != "" {
		if success {
			MongoBackupDuration.Set(time.Since(start).Seconds())
			MongoBackupSuccessTime.SetToCurrentTime()
			MongoBackupSuccess.Set(1)
		} else {
			MongoBackupSuccess.Set(0)
		}

		log.Infof("Sending metrics to pushgateway: %s", pushgatewayURL)
		if err := pusher.Add(); err != nil {
			log.Errorf("Could not send to pushgateway: %v", err)
		}
	}
}

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = logDateLayout
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true
	log.SetOutput(os.Stdout)

	kingpin.Version(ver)
	kingpin.Parse()

	if *verbose {
		log.SetLevel(log.DebugLevel)
	}

	log.Infof("Starting, version %s", ver)

	lockFile := "mongo-backup_" + *mongoClusterName + ".lock"
	lock, err := lockfile.New(filepath.Join(os.TempDir(), lockFile))
	if err != nil {
		log.Fatalf("Cannot initialize lock. reason: %v", err)
	}

	err = lock.TryLock()
	if err != nil {
		log.Fatalf("Cannot lock %v, reason: %v", lock, err)
	}
	defer lock.Unlock()

	pusher, start := pushgatewayInitialize(*pushgatewayURL, "mongo-backup", *mongoClusterName)

	if err := processNodes(
		*mongoClusterName,
		*mongosURL,
		*stopBalancerTimeout,
		*sshUser,
		*sshPort,
		*waitingChefStoppedTimeout,
		*mongoDataDir,
		*dstRootPath,
		*retentionItems,
		*stopMongoDaemonDelay,
		*rsyncThreads,
		*rsyncRetries,
		*amtoolPath,
		*alertmanagerURL,
		lock,
	); err == nil {
		sendPushgatewayMetrics(true, *pushgatewayURL, start, pusher)
		log.Info("Program finished successfully")
	} else {
		sendPushgatewayMetrics(false, *pushgatewayURL, start, pusher)
		sendSlackMsg(*slackURL, *slackChannel, *mongoClusterName, *slackUsername, "danger", *slackIconEmoji, 5)
		log.Errorf("Program finished with error: %v", err)
	}
}
