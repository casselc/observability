// Binary-size probe: minio-go alone.
package main

import "github.com/minio/minio-go/v7"

func main() { minio.New("x", &minio.Options{}) }
