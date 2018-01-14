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
)

const (
	ver string = "0.22"
	dateLayout string = "2006-01-02_150405"
)

var (
	stopBalancerTimeout = kingpin.Flag("timeout", "timeout for stopping mongo balancer in seconds").Default("1800").Short('t').Int()
	waitingChefStoppedTimeout = kingpin.Flag("chef-timeout", "waiting for chef stopped timoue in seconds").Default("900").Int()
	stopMongoDaemonDelay = kingpin.Flag("stop-delay", "delay after chef disabled to stop MongoDB daemon on seconds").Default("240").Int()
	mongosURL = kingpin.Flag("url", "mongos URL").Default("127.0.0.1:27017").Short('u').String()
	sshUser = kingpin.Flag("ssh-user", "ssh user").Default("backup_mongo").String()
	sshPort = kingpin.Flag("ssh-port", "ssh port").Default("22").Short('p').Int()
	mongoClusterName = kingpin.Flag("cluster-name", "MongoDB cluster name").Short('c').Required().String()
	mongoDataDir = kingpin.Flag("data-dir", "MongoDB data directory").Default("/var/lib/mongo/").String()
	dstRootPath = kingpin.Flag("dst-root-path", "destination root path").Default("/data/backup_mongo/files").String()
	retentionItems = kingpin.Flag("retention-items", "number of retention items to keep").Default("5").Int()
	verbose = kingpin.Flag("verbose", "verbose").Short('v').Bool()
	dryRun = kingpin.Flag("dry-run", "dry run").Short('n').Bool()
	rsyncThreads = kingpin.Flag("rsync-threads", "number of concurrent rsync threads").Default("3").Int()
	slackURL = kingpin.Flag("slack-url", "slack URL").Default("http://127.0.0.1").String()
	slackChannel = kingpin.Flag("slack-channel", "slack channel to send messages").Default("#it-automatic-logs").String()
	slackUsername = kingpin.Flag("slack-username", "slack username field").Default("mongo-backup").String()
	slackIconEmoji = kingpin.Flag("slack-icon-emoji", "slack icon-emoji field").Default(":mongo-backup:").String()
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
	nodeName string
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

func getShards(url string) (map[string][]map[string]string, error) {
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
	shards, err := getShards(url)
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

func getSecondaryNodes(url string) (map[string]string, error) {
	shards, err := markMasterShard(url)
	if err != nil {
		return map[string]string{}, err
	}

	secondaryNodes := make(map[string]string)
	for shard, nodes := range shards {
		for _, node := range nodes {
			if _, ok := node["master"]; !ok {
				secondaryNodes[shard] = strings.Split(node["node_url"], ":")[0]
			}
		}
		if _, ok := secondaryNodes[shard]; !ok {
			log.Warnf("No secondary node for shard %s", shard)
		}
	}

	return secondaryNodes, nil
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
	log.Infof("%s: flushing buffers", nodeName)

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

func drainNodes(shards map[string]string, sshUser string, sshPort, waitingChefStoppedTimeout, stopMongoDaemonDelay int) error {
	failed := false
	if err := disableChefOnNodes(shards, sshUser, sshPort, waitingChefStoppedTimeout); err != nil {
		log.Error(err)
		failed = true
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

func disableChefOnNodes(shards map[string]string, sshUser string, sshPort, waitingChefStoppedTimeout int) error {
	result := make(chan error, len(shards))

	for _, nodeName := range shards {
		go disableChef(nodeName, sshUser, sshPort, waitingChefStoppedTimeout, result)
	}

	if !allResultsSuccess(shards, result) {
		return errors.New("Disabling chef failed on one or more nodes")
	}
	return nil
}

func disableMongoOnNodes(shards map[string]string, sshUser string, sshPort int) error {
	result := make(chan error, len(shards))

	for _, nodeName := range shards {
		go disableMongo(nodeName, sshUser, sshPort, result)
	}

	if !allResultsSuccess(shards, result) {
		return errors.New("Disabling mongod failed on one or more nodes")
	}
	return nil
}

func stopMongoDaemonOnNodes(shards map[string]string, sshUser string, sshPort int) error {
	result := make(chan error, len(shards))

	for _, nodeName := range shards {
		go stopMongoDaemon(nodeName, sshUser, sshPort, result)
	}

	if !allResultsSuccess(shards, result) {
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
	}

	result <- nil
}

func activateNodes(shards map[string]string, sshUser string, sshPort int) error {
	result := make(chan error, len(shards))

	for _, nodeName := range shards {
		go activateNode(nodeName, sshUser, sshPort, result)
	}

	if !allResultsSuccess(shards, result) {
		return errors.New("One or more nodes activate failed")
	}
	return nil
}

func allResultsSuccess(shards map[string]string, result <-chan error) bool {
	success := true
	for i := 0; i < len(shards); i++ {
		if err := <-result; err != nil {
			log.Error(err)
			success = false
		}
	}
	return success
}

func clenaup(url string, shards map[string]string, sshUser string, sshPort int) {
	if err := activateNodes(shards, sshUser, sshPort); err != nil {
		log.Error(err)
	}

	if err := startBalancer(url); err != nil {
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

func processNodes(mongoClusterName, url string, stopBalancerTimeout int, sshUser string, sshPort, waitingChefStoppedTimeout int, mongoDataDir, dstRootPath string, retentionItems, stopMongoDaemonDelay, rsyncThreads int, lock lockfile.Lockfile, dryRun bool) error {
	shards, err := getSecondaryNodes(url)
	if err != nil {
		return err
	}

	mongoDataDir = addTrailingSlashIfNotExists(mongoDataDir)
	dstRootPath = deleteTrailingSlashIfExists(dstRootPath)
	dateString := fmt.Sprint(time.Now().Format(dateLayout))

	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		<-sigchan
		log.Error("Program killed!")

		clenaup(url, shards, sshUser, sshPort)

		lock.Unlock()
		os.Exit(1)
	}()

	if err := stopBalancer(url, stopBalancerTimeout); err != nil {
		return err
	}

	var processErr error
	if err := drainNodes(shards, sshUser, sshPort, waitingChefStoppedTimeout, stopMongoDaemonDelay); err == nil {
		if err := rsyncBackups(shards, mongoClusterName, sshUser, sshPort, mongoDataDir, dstRootPath, dateString, rsyncThreads); err != nil {
			log.Error(err)
			processErr = err
		}
	} else {
		log.Error(err)
		processErr = err
		if err := activateNodes(shards, sshUser, sshPort); err != nil {
			log.Error(err)
		}
	}

	if err := startBalancer(url); err != nil {
		return err
	}

	if processErr == nil {
		retentionDataCleanup(dstRootPath + "/" + mongoClusterName, retentionItems)
	} else {
		return processErr
	}

	return nil
}

func rsyncBackups(shards map[string]string, clusterName, sshUser string, sshPort int, srcPath, dstRootPath, dateString string, rsyncThreads int) error {
	jobs := make(chan Job, len(shards))
	results := make(chan Msg, len(shards))

	for w := 1; w <= rsyncThreads; w++ {
		go rsyncWorker(jobs, results)
	}

	for shardName, nodeName := range shards {
		var j Job
		j.clusterName = clusterName
		j.sshUser = sshUser
		j.sshPort = sshPort
		j.srcPath = srcPath
		j.dstRootPath = dstRootPath
		j.shardName = shardName
		j.nodeName = nodeName
		j.dateString = dateString

		jobs <- j
	}
	close(jobs)

	failedNode := false
	for i := 0; i < len(shards); i++ {
		msg := <-results
		if msg.err != nil {
			failedNode = true
		}
	}

	if failedNode {
		return errors.New("One or more rsync failed")
	}

	return nil
}

func rsyncWorker(jobs <-chan Job, results chan<- Msg) {
	for j := range jobs {
		var msg Msg
		msg.node = j.nodeName

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
			log.Infof("Shard %s: performing rsync backup from %s", j.shardName, j.nodeName)
			if err := executeCommand(prepareRsyncCommands(j.clusterName, j.shardName, j.sshUser, j.sshPort, j.nodeName, j.srcPath, dstPath)); err != nil {
				msg.err = err
			}
		}

		if msg.err == nil {
			log.Infof("Shard %s: rsync from %s completed successfully", j.shardName, j.nodeName)
		} else {
			log.Errorf("Shard %s: rsync failed from node %s: %v", j.shardName, msg.node, msg.err)
		}

		result := make(chan error)
		go activateNode(j.nodeName, j.sshUser, j.sshPort, result)
		if err := <-result; err != nil {
			log.Errorf("%s: cannot enable mongo node: %v", j.nodeName, err)
		}

		if msg.err != nil {
			// when one job failed take all jobs to avoid processing another
			for f := range jobs {
				var m Msg
				m.node = f.nodeName
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
	}
	result <- nil
}

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
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
		lock,
		*dryRun,
	); err == nil {
		log.Info("Program finished successfully")
	} else {
		log.Errorf("Program finished with error: %v", err)
		sendSlackMsg(*slackURL, *slackChannel, *mongoClusterName, *slackUsername, "danger", *slackIconEmoji, 5)
	}
}
