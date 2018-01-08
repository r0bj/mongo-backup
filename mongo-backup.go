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
	ver string = "0.11"
	lockFile string = "mongo-backup.lock"
	dateLayout string = "2006-01-02_150405"
)

var (
	stopBalancerTimeout = kingpin.Flag("timeout", "timeout for stopping mongo balancer in seconds").Default("1800").Short('t').Int()
	waitingChefStoppedTimeout = kingpin.Flag("chef-timeout", "waiting for chef stopped timoue in seconds").Default("300").Int()
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

type Command struct {
	cmd string
	args []string
}

type Lock struct {
	ID bson.ObjectId `bson:"_id,omitempty"`
	State int
}

type Msg struct {
	node string
	err error
}

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

type Payload struct {
	Username string `json:"username"`
	Channel string `json:"channel"`
	IconEmoji string `json:"icon_emoji"`
	Attachments []Attachment `json:"attachments"`
}

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
		return fmt.Errorf("Cannot disable balancer, if balancer was never disabled on this cluster try enable and disable it manually: %v", err)
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

	if unlocked {
		return nil
	} else {
		return errors.New("Balancer is still locked after timeout")
	}
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

	for k, _ := range data {
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

func prepareSshCommands(user, host string, port int, remoteCmd []string) Command {
	var command Command
	command.cmd = "ssh"
	command.args = []string{
		"-o",
		"StrictHostKeyChecking=no",
		"-o",
		"PasswordAuthentication=no",
		"-p",
		strconv.Itoa(port),
		"-l",
		user,
		host,
	}
	command.args = append(command.args, remoteCmd...)

	return command
}

func disableChef(user, host string, port int) error {
	log.Infof("%s: disabling chef-client", host)
	if err := executeCommand(prepareSshCommands(user, host, port, []string{"sudo", "touch", "/etc/disabled/chef"})); err != nil {
		return err
	}

	return nil
}

func disableMongo(user, host string, port int) error {
	log.Infof("%s: disabling mongo", host)
	if err := executeCommand(prepareSshCommands(user, host, port, []string{"sudo", "touch", "/etc/disabled/mongo"})); err != nil {
		return err
	}

	return nil
}

func stopMongoDaemon(user, host string, port int) error {
	log.Infof("%s: stopping mongo daemon", host)
	if err := executeCommand(prepareSshCommands(user, host, port, []string{"sudo", "systemctl", "stop", "mongod.service"})); err != nil {
		return err
	}

	return nil
}

func startMongoDaemon(user, host string, port int) error {
	log.Infof("%s: starting mongo daemon", host)
	if err := executeCommand(prepareSshCommands(user, host, port, []string{"sudo", "systemctl", "start", "mongod.service"})); err != nil {
		return err
	}

	return nil
}

func enableChef(user, host string, port int) error {
	log.Infof("%s: enabling chef-client", host)
	if err := executeCommand(prepareSshCommands(user, host, port, []string{"sudo", "rm", "-f", "/etc/disabled/chef"})); err != nil {
		return err
	}

	return nil
}

func enableMongo(user, host string, port int) error {
	log.Infof("%s: enabling mongo", host)
	err := executeCommand(prepareSshCommands(user, host, port, []string{"sudo", "rm", "-f", "/etc/disabled/mongo"}))
	if err != nil {
		return err
	}

	return nil
}

func flushBuffers(user, host string, port int) error {
	log.Infof("%s: flushing buffers", host)
	err := executeCommand(prepareSshCommands(user, host, port, []string{"sync"}))
	if err != nil {
		return err
	}

	return nil
}

func waitingChefStopped(user, host string, port, waitingChefStoppedTimeout int) error {
	log.Infof("%s: waiting chef-client not running", host)

	for i := 0; i < waitingChefStoppedTimeout; i++ {
		err := executeCommand(prepareSshCommands(user, host, port, []string{"sudo", "pgrep", "chef-client"}))
		if err != nil {
			return nil
		}
		log.Debugf("Waiting for chef-client stopped: %d seconds", i)
		time.Sleep(time.Second)
	}

	return errors.New("Chef-client still running after timeout")
}

func disableMongoNode(nodeName, sshUser string, sshPort, waitingChefStoppedTimeout, stopMongoDaemonDelay int, result chan<- error) {
	if err := disableChef(sshUser, nodeName, sshPort); err != nil {
		result <- fmt.Errorf("%s: cannot disable chef-client", nodeName)
		return
	}

	if err := waitingChefStopped(sshUser, nodeName, sshPort, waitingChefStoppedTimeout); err != nil {
		result <- fmt.Errorf("%s: waiting chef-client stopped timeout", nodeName)
		return 
	}

	if err := disableMongo(sshUser, nodeName, sshPort); err != nil {
		result <- fmt.Errorf("%s: cannot disable mongo", nodeName)
		return
	}

	log.Infof("%s: waiting delay %d seconds", nodeName, stopMongoDaemonDelay)
	time.Sleep(time.Second * time.Duration(stopMongoDaemonDelay))
	if err := stopMongoDaemon(sshUser, nodeName, sshPort); err != nil {
		result <- fmt.Errorf("%s: cannot stop mongo daemon", nodeName)
		return
	}

	if err := flushBuffers(sshUser, nodeName, sshPort); err != nil {
		result <- fmt.Errorf("%s: cannot flush buffers", nodeName)
		time.Sleep(time.Second * 10)
		return
	}

	result <- nil
}

func disableMongoNodes(shards map[string]string, sshUser string, sshPort, waitingChefStoppedTimeout, stopMongoDaemonDelay int) error {
	result := make(chan error, len(shards))

	for _, nodeName := range shards {
		go disableMongoNode(nodeName, sshUser, sshPort, waitingChefStoppedTimeout, stopMongoDaemonDelay, result)
	}

	failed := false
	for i := 0; i < len(shards); i++ {
		if err := <-result; err != nil {
			log.Error(err)
			failed = true
		}
	}

	if failed {
		return errors.New("Disabling MongoDB failed on one or more nodes")
	}
	return nil
}

func enableMongoNode(nodeName, sshUser string, sshPort int) error {
	if err := enableChef(sshUser, nodeName, sshPort); err != nil {
		return fmt.Errorf("%s: cannot enable chef-client", nodeName)
	}

	if err := enableMongo(sshUser, nodeName, sshPort); err != nil {
		return fmt.Errorf("%s: cannot enable mongo", nodeName)
	}

	if err := startMongoDaemon(sshUser, nodeName, sshPort); err != nil {
		return fmt.Errorf("%s: cannot start mongo daemon", nodeName)
	}

	return nil
}

func enableMongoNodeConcurrent(nodeName, sshUser string, sshPort, waitingChefStoppedTimeout int, result chan<- error) {
	if err := enableChef(sshUser, nodeName, sshPort); err != nil {
		result <- fmt.Errorf("%s: cannot enable chef-client", nodeName)
		return
	}

	if err := enableMongo(sshUser, nodeName, sshPort); err != nil {
		result <- fmt.Errorf("%s: cannot enable mongo", nodeName)
		return
	}

	if err := startMongoDaemon(sshUser, nodeName, sshPort); err != nil {
		result <- fmt.Errorf("%s: cannot start mongo daemon", nodeName)
		return
	}

	result <- nil
}

func enableMongoNodesConcurrent(shards map[string]string, sshUser string, sshPort, waitingChefStoppedTimeout int) error {
	result := make(chan error, len(shards))

	for _, nodeName := range shards {
		go enableMongoNodeConcurrent(nodeName, sshUser, sshPort, waitingChefStoppedTimeout, result)
	}

	failed := false
	for i := 0; i < len(shards); i++ {
		if err := <-result; err != nil {
			log.Error(err)
			failed = true
		}
	}

	if failed {
		return errors.New("Enabling MongoDB failed on one or more nodes")
	}
	return nil
}

func clenaup(url string, shards map[string]string, sshUser string, sshPort, waitingChefStoppedTimeout int) {
	if err := enableMongoNodesConcurrent(shards, sshUser, sshPort, waitingChefStoppedTimeout); err != nil {
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

		clenaup(url, shards, sshUser, sshPort, waitingChefStoppedTimeout)

		lock.Unlock()
		os.Exit(1)
	}()

	if err := stopBalancer(url, stopBalancerTimeout); err != nil {
		return err
	}

	var processErr error
	if err := disableMongoNodes(shards, sshUser, sshPort, waitingChefStoppedTimeout, stopMongoDaemonDelay); err == nil {
		if err := rsyncBackups(shards, mongoClusterName, sshUser, sshPort, mongoDataDir, dstRootPath, dateString, rsyncThreads); err != nil {
			log.Error(err)
			processErr = err
		}
	} else {
		log.Error(err)
		processErr = err
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
			log.Infof("Performing rsync backup from %s", j.nodeName)
			if err := executeCommand(prepareRsyncCommands(j.clusterName, j.shardName, j.sshUser, j.sshPort, j.nodeName, j.srcPath, dstPath)); err != nil {
				msg.err = err
			}
		}

		if msg.err == nil {
			log.Infof("Rsync from %s completed successfully", j.nodeName)
		} else {
			log.Errorf("Rsync failed from node %s: %v", msg.node, msg.err)
		}

		if err := enableMongoNode(j.nodeName, j.sshUser, j.sshPort); err != nil {
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

func sendSlackMsg(webhookUrl, channel, cluster, username, color, iconEmoji string, timeout int) error {
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
	go httpPost(webhookUrl, string(json), result)

	select {
	case err := <-result:
		return err
	case <-time.After(time.Second * time.Duration(timeout)):
		return errors.New("Slack connections timeout")
	}
}

func httpPost(url, data string, result chan error) {
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

	log.Info("Starting...")

	lock, err := lockfile.New(filepath.Join(os.TempDir(), lockFile))
	if err != nil {
		log.Fatalf("Cannot init lock. reason: %v", err)
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
	); err != nil {
		log.Error(err)
		sendSlackMsg(*slackURL, *slackChannel, *mongoClusterName, *slackUsername, "danger", *slackIconEmoji, 5)
	}
}
