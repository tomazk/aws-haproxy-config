package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"text/template"
	"time"

	"github.com/tomazk/envcfg"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const defaultWaitTimeSeconds = 10

var haProxyTemplate = template.Must(
	template.ParseFiles("haproxy.cfg.template"),
)

type env struct {
	AwsAccessKeyID      string `envcfg:"AWS_ACCESS_KEY_ID" envcfgkeep:""`
	AwsSecretAccessKey  string `envcfg:"AWS_SECRET_ACCESS_KEY" envcfgkeep:""`
	AwsSqsRegion        string `envcfg:"AWS_SQS_REGION"`
	AwsSqsQueueName     string `envcfg:"AWS_SQS_QUEUE_NAME"`
	AwsSnsTopicName     string `envcfg:"AWS_SNS_TOPIC_NAME"`
	AwsEC2GroupName     string `envcfg:"AWS_EC2_GROUP_NAME"`
	HaproxyFileDest     string `envcfg:"HAPROXY_FILE_DEST"`
	HaproxyReloadScript string `envcfg:"HAPROXY_RELOAD_SCRIPT"`
}

type snsMsg struct {
	Type      string
	MessageID string
	TopicArn  string
	Timestamp time.Time
	Subject   string
	Message   string
}

type internalInstance struct {
	internalDNS  string
	internalIP   string
	instanceType string
	instanceID   string
	name         string
}

type templateItem struct {
	Name string
	Host string
}

func (i *internalInstance) getName() string {
	if i.name != "" {
		return i.name
	}
	return i.instanceType + i.instanceID
}

func (i *internalInstance) getEndpoint() string {
	// TODO: make this cleaner
	return i.internalIP
}

func reloadHaproxy(pathToScript string) {
	reloadCommand := exec.Command(pathToScript)
	log.Println("executing: ", pathToScript)

	output, err := reloadCommand.CombinedOutput()
	if err != nil {
		log.Println("error when running:", err)
		return
	}

	log.Printf("output of command %v: %v\n", pathToScript, string(output))
}

func validateMsg(msg *sqs.Message) bool {
	// TODO: better valitation required
	msgBody := &snsMsg{}
	err := json.Unmarshal([]byte(*msg.Body), &msgBody)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func getEC2Config(ec2Client *ec2.EC2, awsEC2GroupName string) ([]templateItem, error) {

	var templateList []templateItem
	internalInstances, err := getInstanceListFromGroup(ec2Client, awsEC2GroupName)
	if err != nil {
		log.Println("error when getting EC2 data: ", err)
		return nil, err
	}

	for _, instance := range internalInstances {
		templateList = append(templateList, templateItem{
			Name: instance.getName(),
			Host: instance.getEndpoint(),
		})
	}

	return templateList, nil

}

func writeHaproxyConfig(haproxyFileDest string, templateData []templateItem) error {

	haproxyConfigFile, err := os.Create(haproxyFileDest)
	if err != nil {
		log.Println("error when creating config file: ", err)
		return err
	}

	err = haProxyTemplate.Execute(haproxyConfigFile, templateData)
	if err != nil {
		log.Println("error when writing to file: ", err)
		return err
	}
	log.Println("config template populated with: ", templateData)

	return nil
}

func handleMessage(ec2Client *ec2.EC2, msg *sqs.Message, environ *env) {

	if !validateMsg(msg) {
		log.Printf("msg invalid: %#v", msg)
		return
	}

	templateData, err := getEC2Config(ec2Client, environ.AwsEC2GroupName)
	if err != nil {
		return
	}

	err = writeHaproxyConfig(environ.HaproxyFileDest, templateData)
	if err != nil {
		return
	}

	reloadHaproxy(environ.HaproxyReloadScript)
}

func getInstanceListFromGroup(ec2Client *ec2.EC2, groupName string) ([]*internalInstance, error) {

	var instances []*internalInstance

	output, err := ec2Client.DescribeInstances(&ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("tag:group"),
				Values: []*string{aws.String(groupName)},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	for _, reservation := range output.Reservations {
		for _, instance := range reservation.Instances {
			instanceIsRelevant := false
			instanceObj := &internalInstance{}

			instanceObj.instanceID = *instance.InstanceId
			instanceObj.instanceType = *instance.InstanceType

			for _, tag := range instance.Tags {
				if *tag.Key == "group" && *tag.Value == groupName {
					instanceIsRelevant = true
				}
				if *tag.Key == "Name" {
					instanceObj.name = *tag.Value
				}
			}
			if instanceIsRelevant &&
				(*instance.State.Name == ec2.InstanceStateNameRunning ||
					*instance.State.Name == ec2.InstanceStateNamePending) {

				instanceObj.internalDNS = *instance.PrivateDnsName
				instanceObj.internalIP = *instance.PrivateIpAddress

				log.Println("found instance: ", *instanceObj)
				instances = append(instances, instanceObj)
			}
		}
	}

	return instances, nil
}

func getQueueURL(sqsClient *sqs.SQS, awsSqsQueueName string) (*string, error) {
	queueURLObj, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(awsSqsQueueName),
	})
	if err != nil {
		return nil, err
	}

	return queueURLObj.QueueUrl, nil
}

func main() {

	// get env varaibles
	environ := &env{}
	err := envcfg.Unmarshal(&environ)
	envcfg.ClearEnvVars(&environ)
	if err != nil {
		log.Fatalln(err)
	}

	// establish session and get client
	session := session.New(&aws.Config{
		Credentials: credentials.NewEnvCredentials(),
		Region:      aws.String(environ.AwsSqsRegion),
	})
	sqsClient := sqs.New(session)
	ec2Client := ec2.New(session)

	queueURL, err := getQueueURL(sqsClient, environ.AwsSqsQueueName)
	if err != nil {
		log.Println("no queue found: ", environ.AwsSqsQueueName)
		log.Fatalln(err)
	}

	// start consume
	log.Println("consume from queue:", *queueURL)
	for {
		resp, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:        queueURL,
			WaitTimeSeconds: aws.Int64(defaultWaitTimeSeconds),
		})
		if err != nil {
			fmt.Println("error when recieving message", err)
			continue
		}

		for _, msg := range resp.Messages {
			handleMessage(ec2Client, msg, environ)
			sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      queueURL,
				ReceiptHandle: msg.ReceiptHandle,
			})
		}
	}
}
