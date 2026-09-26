package commit

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3Store is a Store on a bucket (aws-sdk-go-v2, DECISIONS.md D18).
type S3Store struct {
	Client *s3.Client
	Bucket string
	// OnError, if set, sees every PUT error that becomes PutUnknown.
	OnError func(key string, err error)
}

// Classify maps an SDK error onto a PUT outcome.
func Classify(err error) PutOutcome {
	if err == nil {
		return PutOK
	}
	var re *smithyhttp.ResponseError
	status := 0
	if errors.As(err, &re) {
		status = re.HTTPStatusCode()
	}
	code := ""
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code = ae.ErrorCode()
	}
	if status == http.StatusPreconditionFailed || code == "PreconditionFailed" {
		return PutExists
	}
	return PutUnknown
}

func isNotFound(err error) bool {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	var ae smithy.APIError
	return errors.As(err, &ae) && (ae.ErrorCode() == "NotFound" || ae.ErrorCode() == "NoSuchKey")
}

func (s *S3Store) PutCreate(ctx context.Context, key string, body []byte, contentType string, meta map[string]string) PutOutcome {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(s.Bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		IfNoneMatch:   aws.String("*"),
		Metadata:      meta,
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	_, err := s.Client.PutObject(ctx, in)
	o := Classify(err)
	if o == PutUnknown && s.OnError != nil {
		s.OnError(key, err)
	}
	return o
}

func (s *S3Store) Head(ctx context.Context, key string) (map[string]string, bool, error) {
	out, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	m := make(map[string]string, len(out.Metadata))
	for k, v := range out.Metadata {
		m[strings.ToLower(k)] = v
	}
	return m, true, nil
}
