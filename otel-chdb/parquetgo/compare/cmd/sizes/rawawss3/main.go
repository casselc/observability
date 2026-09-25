// Binary-size probe: the aws-sdk-go-v2 S3 client alone.
package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	c := s3.New(s3.Options{Region: "x", BaseEndpoint: aws.String("http://x"), UsePathStyle: true})
	c.PutObject(context.Background(), &s3.PutObjectInput{})
}
