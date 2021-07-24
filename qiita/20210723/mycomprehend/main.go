package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/comprehend"
	comprehend_types "github.com/aws/aws-sdk-go-v2/service/comprehend/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Entity struct {
	BeginOffset int     `json:"BeginOffset"`
	EndOffset   int     `json:"EndOffset"`
	Score       float64 `json:"Score"`
	Text        string  `json:"Text"`
	Type        string  `json:"Type"`
}

type JsonOutput struct {
	File     string   `json:"File"`
	Entities []Entity `json:"Entities"`
}

const (
	resultFileName string = "output"
	bufferSize     int    = 1024 * 1024
	interval       int    = 10
	timeoutCount   int    = 100
	jobName        string = "sample-entities-detection-job"
	// ここのロールは置き換えること
	roleArn string = "arn:aws:iam::123456789012:role/comprehend-access-role"
)

func main() {
	pBucketName := flag.String("bucket", "testcomprehend-tn", "Bucket to put a content on.")
	pPrefixName := flag.String("prefix", "comprehend/", "Prefix to store a content file")
	pContentFileName := flag.String("file", "content.txt", "The content file")
	pLimitNumber := flag.Int("limit", 20, "The limit to display keywords")

	flag.Parse()

	fmt.Println("START!!!!")
	fmt.Println("Bucket:", *pBucketName)
	fmt.Println("Prefix:", *pPrefixName)
	fmt.Println("Limit:", *pLimitNumber)
	fmt.Println("File:", *pContentFileName)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	s3Client := s3.NewFromConfig(cfg)
	comprehendClient := comprehend.NewFromConfig(cfg)

	// 処理対象ファイルをS3にアップロードする
	file, err := os.Open(*pContentFileName)
	if err != nil {
		fmt.Println("Unable to open file " + *pContentFileName)
		return
	}
	defer file.Close()

	objectName := *pPrefixName + "input/" + *pContentFileName
	fmt.Println("Object Name: " + objectName)
	input := &s3.PutObjectInput{
		Bucket: pBucketName,
		Key:    &objectName,
		Body:   file,
	}

	_, err = s3Client.PutObject(context.TODO(), input)
	if err != nil {
		fmt.Println("Got error uploading file:")
		fmt.Println(err)
		return
	}
	// Amazon Comprehend 呼び出し
	inputS3Uri := "s3://" + *pBucketName + "/" + objectName
	fmt.Println("InputS3URI: " + inputS3Uri)
	inputConfig := &comprehend_types.InputDataConfig{
		S3Uri:       &inputS3Uri,
		InputFormat: comprehend_types.InputFormatOneDocPerFile,
	}
	outputS3Uri := "s3://" + *pBucketName + "/" + *pPrefixName + "output/"
	fmt.Println("OutputS3URI: " + outputS3Uri)
	outputConfig := &comprehend_types.OutputDataConfig{
		S3Uri: &outputS3Uri,
	}
	roleArnForInput := roleArn
	jobNameForInput := jobName
	jobInput := &comprehend.StartEntitiesDetectionJobInput{
		DataAccessRoleArn: &roleArnForInput,
		InputDataConfig:   inputConfig,
		LanguageCode:      comprehend_types.LanguageCodeJa,
		OutputDataConfig:  outputConfig,
		JobName:           &jobNameForInput,
	}
	out, err := comprehendClient.StartEntitiesDetectionJob(context.TODO(), jobInput)
	if err != nil {
		fmt.Println("Starting an entities detection job Error:")
		fmt.Println(err)
		return
	}
	jobId := *out.JobId
	fmt.Println("Job ID: " + jobId)

	// Amazon Comprehend 完了確認
	describeJobInput := &comprehend.DescribeEntitiesDetectionJobInput{
		JobId: &jobId,
	}
	var outDesc *comprehend.DescribeEntitiesDetectionJobOutput
	for i := 0; i < timeoutCount; i++ {
		fmt.Println("In Progress...")
		time.Sleep(time.Duration(interval) * time.Second)
		outDesc, err = comprehendClient.DescribeEntitiesDetectionJob(context.TODO(), describeJobInput)
		if err != nil {
			fmt.Println("Getting a status of the entities detection job Error:")
			fmt.Println(err)
			return
		}
		if outDesc.EntitiesDetectionJobProperties.JobStatus == comprehend_types.JobStatusCompleted {
			fmt.Println("Job Completed.")
			break
		}
	}
	if outDesc.EntitiesDetectionJobProperties.JobStatus != comprehend_types.JobStatusCompleted {
		fmt.Println("Job Timeout.")
		return
	}

	// Amazon Comprehend 結果ダウンロード
	downloadS3Uri := *outDesc.EntitiesDetectionJobProperties.OutputDataConfig.S3Uri
	fmt.Println("Output S3 URI: " + downloadS3Uri)
	parsedUri, err := url.Parse(downloadS3Uri)
	if err != nil {
		fmt.Println("URI Parse Error:")
		fmt.Println(err)
		return
	}
	downloadObjectName := parsedUri.Path[1:]
	fmt.Println("Download Object Name: " + downloadObjectName)

	getObjectInput := &s3.GetObjectInput{
		Bucket: pBucketName,
		Key:    &downloadObjectName,
	}
	outGetObject, err := s3Client.GetObject(context.TODO(), getObjectInput)
	if err != nil {
		fmt.Println("Getting S3 object Error:")
		fmt.Println(err)
		return
	}
	defer outGetObject.Body.Close()

	// tar.gzファイルから結果のJSONを取り出す
	gzipReader, err := gzip.NewReader(outGetObject.Body)
	if err != nil {
		fmt.Println("Reading a gzip file Error:")
		fmt.Println(err)
		return
	}
	defer gzipReader.Close()
	tarfileReader := tar.NewReader(gzipReader)

	var jsonOutput JsonOutput
	for {
		tarfileHeader, err := tarfileReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Reading a tar file Error:")
			fmt.Println(err)
			return
		}

		if tarfileHeader.Name == resultFileName {
			jsonReader := bufio.NewReaderSize(tarfileReader, bufferSize)
			for {
				// TODO: isPrefixを見て、行がバッファに対して長すぎる場合の処理を行なう
				line, _, err := jsonReader.ReadLine()
				if err == io.EOF {
					break
				}
				if err != nil {
					fmt.Println("Reading a line Error:")
					fmt.Println(err)
					return
				}
				json.Unmarshal([]byte(line), &jsonOutput)
				fmt.Println()
				fmt.Println("File: " + jsonOutput.File)
				// Score順にソートする
				entities := jsonOutput.Entities
				sort.Slice(entities, func(i, j int) bool { return entities[i].Score > entities[j].Score })
				// Amazon Comprehend 結果表示
				for i := 0; i < *pLimitNumber; i++ {
					fmt.Printf("%s (%s): %f\n", entities[i].Text, entities[i].Type, entities[i].Score)
				}
			}
		}
	}

	fmt.Println()
	fmt.Println()
	fmt.Println("END!!!!")
}
