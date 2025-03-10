package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

func main() {
  token := "doicCBo60G0l9OazXXQs-7iVTaO5fuvU1GFuzwFbLkHHWvjL4HDi8mgfx7wr8TBqkxwOnZMk2M6NCCQ6r3Hz_Q=="//os.Getenv("INFLUXDB_TOKEN")
  url := "http://192.168.66.11:8086"
  client := influxdb2.NewClient(url, token)
  org := "docs"
bucket := "processdata"
thingId := uuid.New()
unique := "Temperature"
writeAPI := client.WriteAPIBlocking(org, bucket)
for value := 0; value < 5000000; value++ {
	temperature := randFloats(-10.0, 35.0, 1 )
	tags := map[string]string{
		thingId.String(): unique,
	}
	fmt.Println(strconv.FormatFloat(temperature[0], 'f', -1, 64))
	fields := map[string]interface{}{
		"value": temperature[0],
	}
	point := write.NewPoint("measurement", tags, fields, time.Now())
	time.Sleep(1 * time.Second) // separate points by 1 second

	if err := writeAPI.WritePoint(context.Background(), point); err != nil {
		log.Fatal(err)
	}
	}
	queryAPI := client.QueryAPI(org)
query := `from(bucket: "processdata")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "measurement")`
results, err := queryAPI.Query(context.Background(), query)
if err != nil {
    log.Fatal(err)
}
for results.Next() {
    fmt.Println(results.Record())
}
if err := results.Err(); err != nil {
    log.Fatal(err)
}
query = `from(bucket: "processdata")
              |> range(start: -10m)
              |> filter(fn: (r) => r._measurement == "measurement")
              |> mean()`
results, err = queryAPI.Query(context.Background(), query)
if err != nil {
    log.Fatal(err)
}
for results.Next() {
    fmt.Println(results.Record())
}
if err := results.Err(); err != nil {
    log.Fatal(err)
}
}

func randFloats(min, max float64, n int) []float64 {
    res := make([]float64, n)
    for i := range res {
        res[i] = min + rand.Float64() * (max - min)
    }
    return res
}