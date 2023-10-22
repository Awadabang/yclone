package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync-worker/local"
	"sync-worker/operations"

	"github.com/rclone/rclone/backend/googlecloudstorage"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type GCS string

func (c GCS) Get(key string) (value string, ok bool) {
	switch key {
	case "anonymous":
		return "true", true
	default:
		return "", false
	}
}

var (
	purlTypes []string
)

func main() {
	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017/"))
	if err != nil {
		log.Fatal(err)
	}
	packageLicneseColl := mongoClient.Database("test").Collection("test")
	licneseColl := mongoClient.Database("test").Collection("license")

	// loc, _ := time.LoadLocation("Asia/Shanghai")
	// cronRunner := cron.New(cron.WithLocation(loc))
	// cronRunner.AddFunc("40 2 * * *", func() {
	// 	mongoEx("prod-export-license-bucket-1a6c642fc4de57d4/v1/", []string{"go", "maven"}, "./temp", coll)
	// })
	// cronRunner.AddFunc("40 2 * * *", func() {
	// 	httpEx(context.Background(), coll)
	// })
	// cronRunner.Run()
	// select {}
	mongoEx("prod-export-license-bucket-1a6c642fc4de57d4/v1/", []string{"go", "maven"}, "./temp", packageLicneseColl)
	httpEx(context.Background(), licneseColl)
}

type License struct {
	Name    string `json:"name" bson:"name"`
	Version string `json:"version" bson:"version"`
	License string `json:"license" bson:"license"`
}

func mongoEx(gscPath string, purlTypes []string, localPath string, coll *mongo.Collection) error {
	cm := NewConfigMap()
	cm.AddGetter(GCS("test"), PriorityMax)
	mongoFunc := local.MongoFunc(func(ctx context.Context, path string, reader io.Reader) {
		log.Println("local path", path)
		docs := make([]any, 0)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			dataArr := strings.Split(scanner.Text(), ",")
			if len(dataArr) == 3 {
				if len(dataArr[0]) != 0 && len(dataArr[1]) != 0 && len(dataArr[2]) != 0 {
					if dataArr[2] != "unknown" {
						docs = append(docs, License{
							Name:    dataArr[0],
							Version: dataArr[1],
							License: dataArr[2],
						})
					}
				}
			}
		}
		if err := scanner.Err(); err != nil {
			log.Println("error scanning", err)
		}
		_, err := coll.InsertMany(ctx, docs)
		if err != nil {
			log.Println("insert mongo err", err)
		}
	})
	wg := sync.WaitGroup{}
	for _, purlType := range purlTypes {
		purlType := purlType
		wg.Add(1)
		go func() {
			defer wg.Done()
			localF, err := local.NewFs(context.Background(), "", path.Join(localPath, purlType), cm, mongoFunc)
			if err != nil {
				log.Println(err)
				return
			}
			root, err := url.JoinPath(gscPath, "/", purlType)
			if err != nil {
				log.Println(err)
				return
			}
			gcsFs, err := googlecloudstorage.NewFs(context.Background(), "gcs", root, cm)
			if err != nil {
				log.Println(err)
				return
			}

			err = Sync(context.Background(), localF, gcsFs, false)
			if err != nil {
				log.Println(err)
				return
			}
		}()
	}
	wg.Wait()
	return nil
}

func httpEx(ctx context.Context, coll *mongo.Collection) error {
	buf := new(bytes.Buffer)
	err := operations.CopyURLToWriter(context.Background(), "https://spdx.org/licenses/licenses.json", buf)
	if err != nil {
		return err
	}
	type License struct {
		LicenseListVersion    string   `json:"licenseListVersion" bson:"licenseListVersion"`
		Reference             string   `json:"reference" bson:"reference"`
		IsDeprecatedLicenseId bool     `json:"isDeprecatedLicenseId" bson:"isDeprecatedLicenseId"`
		DetailsUrl            string   `json:"detailsUrl" bson:"detailsUrl"`
		ReferenceNumber       int      `json:"referenceNumber" bson:"referenceNumber"`
		Name                  string   `json:"name" bson:"name"`
		LicenseId             string   `json:"licenseId" bson:"licenseId"`
		SeeAlso               []string `json:"seeAlso" bson:"seeAlso"`
		IsOsiApproved         bool     `json:"isOsiApproved" bson:"isOsiApproved"`
	}
	type Result struct {
		LicenseListVersion string    `json:"licenseListVersion"`
		Licenses           []License `json:"licenses"`
	}
	resultBytes, err := io.ReadAll(buf)
	if err != nil {
		return err
	}
	var res Result
	err = json.Unmarshal(resultBytes, &res)
	if err != nil {
		return err
	}
	docs := make([]any, 0)
	for _, license := range res.Licenses {
		docs = append(docs, License{
			LicenseListVersion:    res.LicenseListVersion,
			Reference:             license.Reference,
			IsDeprecatedLicenseId: license.IsDeprecatedLicenseId,
			DetailsUrl:            license.DetailsUrl,
			ReferenceNumber:       license.ReferenceNumber,
			Name:                  license.Name,
			LicenseId:             license.LicenseId,
			SeeAlso:               license.SeeAlso,
			IsOsiApproved:         license.IsOsiApproved,
		})
	}
	count, err := coll.CountDocuments(ctx, primitive.M{
		"licenseListVersion": res.LicenseListVersion,
	})
	if err != nil {
		return err
	}
	if count == 0 {
		_, err = coll.InsertMany(ctx, docs)
		if err != nil {
			return err
		}
		_, err = coll.DeleteMany(ctx, primitive.M{
			"licenseListVersion": primitive.M{
				"$ne": res.LicenseListVersion,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
