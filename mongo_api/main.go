package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)


type moneothing struct {
	Id int64 `json:"id"`
	ThingId uuid.UUID `json:"thingid"`
	UniqueIdentifier string  `json:"uniqueidentifier"`
	DisplayName string `json:"displayname"`
	Data []moneothingrawdata `json:"Data"`
}

type rawdata struct{
	Id int64 `json:"id"`
	Value string `json:"value"`
	Data []moneothingrawdata `json:"Data"`
}

type moneothingrawdata struct{
	Id int64 `json:"id"`
	ThingId int64 `json:"thingid"`
	RawDataId int64 `json:"rawdataid"`
	TimeStamp int64 `json:"timestamp"`
	Rawdata rawdata `json:"rawdata"`
	MoneoThing moneothing `json:"moneothing"`
}

type moneothingwithvalue struct{
	ThingId uuid.UUID `json:"thingid"`
	UniqueIdentifier string  `json:"uniqueidentifier"`
	DisplayName string `json:"displayname"`
	Value string `json:"value"`
	TimeStamp time.Time `json:"timestamp"`
}

type valuesearchdto struct{
	Value string `json:"value"`
	PageNumber int `json:"pagenumber"`
	PageSize int `json:"pagesize"`
}

type timestamprangesearchdto struct{
	From time.Time `json:"from"`
	To time.Time `json:"to"`
	PageNumber int `json:"pagenumber"`
	PageSize int `json:"pagesize"`
}

type timestampsearchdto struct{
	Time time.Time `json:"time"`
	Lower bool `json:"lower"`
	PageNumber int `json:"pagenumber"`
	PageSize int `json:"pagesize"`
}

type moneothingsearchdto struct{
	ThingId uuid.UUID `json:"thingid"`
	UniqueIdentifier string  `json:"uniqueidentifier"`
	PageNumber int `json:"pagenumber"`
	PageSize int `json:"pagesize"`
}

var moneothings = []moneothing{
	{Id: 1, ThingId: uuid.New(), UniqueIdentifier: "Unique1", DisplayName: "Temperature1"},
	{Id: 2, ThingId: uuid.New(), UniqueIdentifier: "Unique2", DisplayName: "Temperature2"},
	{Id: 3, ThingId: uuid.New(), UniqueIdentifier: "Unique3", DisplayName: "Temperature3"},
}

var rawdatas = []rawdata{
	{Id: 1, Value: "1.345"},
	{Id: 2, Value: "2.54"},
	{Id: 3, Value: "7.98"},
}



func getMoneoThings(c *gin.Context) {
	now := time.Now()
	log.Printf("----> Starting getMoneoThings at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothing")

	log.Println("Executing query")
	cur, err := collection.Find(context.Background(), bson.D{})
	log.Println("Executed query")
	var results = []moneothing{}
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
    c.IndentedJSON(http.StatusOK, results)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThings at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

func getRawData(c *gin.Context) {
	now := time.Now()
	log.Printf("----> Starting getRawData at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("rawdata")

	log.Println("Executing query")
	cur, err := collection.Find(context.Background(), bson.D{})
	log.Println("Executed query")
	var results = []rawdata{}
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
    c.IndentedJSON(http.StatusOK, results)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getRawData at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

func getMoneoThingRawData(c *gin.Context) {
	now := time.Now()
	log.Printf("----> Starting getMoneoThingRawData at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothingrawdata")

	log.Println("Executing query")
	cur, err := collection.Find(context.Background(), bson.D{})
	log.Println("Executed query")
	var results = []moneothingrawdata{}
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
    c.IndentedJSON(http.StatusOK, results)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingRawData at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur)
}

func getRawDataByValue(c *gin.Context) {
	value := c.Param("value")
    now := time.Now()
	log.Printf("----> Starting getRawDataByValue at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("rawdata")

	var result = rawdata{}
	log.Println("Executing query")
	collection.FindOne(context.TODO(), bson.M{"value": value}).Decode(&result)
	log.Println("Executed query")
	
    c.IndentedJSON(http.StatusOK, result)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getRawDataByValue at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

func getMoneoThingRawDataByTimeRange(c *gin.Context) {
	var body timestamprangesearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    now := time.Now()
	log.Printf("----> Starting getMoneoThingRawDataByTimeRange at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothingwithrawdataextended")

	pageOptions := options.Find()
 	pageOptions.SetSkip(int64(body.PageNumber * body.PageSize)) //0-i
 	pageOptions.SetLimit(int64(body.PageSize))
	
	from := body.From.UnixMilli()
	to := body.To.UnixMilli()
	
	filter := bson.M{"timestamp": bson.M{"$gte": from, "$lte": to}}
	
	log.Println("Executing query")
	cur, err := collection.Find(context.TODO(), filter, pageOptions)
	log.Println("Executed query")
	var results = []moneothingwithvalue{}
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
	
    c.IndentedJSON(http.StatusOK, results)

	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingRawDataByTimeRange at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}


func getMoneoThingRawDataByTimeStamp(c *gin.Context) {
	var body timestampsearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    now := time.Now()
	log.Printf("----> Starting getMoneoThingRawDataByTimeStamp at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil{
		panic(err)
	}
														
	collection := db.Database("processdata").Collection("moneothingwithrawdataextended")

	 pageOptions := options.Find()
 	pageOptions.SetSkip(int64(body.PageNumber * body.PageSize)) //0-i
 	pageOptions.SetLimit(int64(body.PageSize))

	var lower string
	if body.Lower{
		lower = "$lte"
	}else{
		lower = "$gte"
	}
	
	timestamp := body.Time.UnixMilli()
	filter := bson.M{"timestamp": bson.M{lower: timestamp}}
	log.Println("Executing query")
	cur, err := collection.Find(context.TODO(), filter, pageOptions)
	log.Println("Executed query")
	var results = []moneothingwithvalue{}
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
	
    c.IndentedJSON(http.StatusOK, results)

	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingRawDataByTimeStamp at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

func getMoneoThingByThingAndUnique(c *gin.Context){
	var body moneothingsearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    now := time.Now()
	log.Printf("----> Starting getMoneoThingByThingAndUnique by thing at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	pageOptions := options.Find()
 	pageOptions.SetSkip(int64(body.PageNumber * body.PageSize))
 	pageOptions.SetLimit(int64(body.PageSize))

	regex := fmt.Sprintf("^%s", body.UniqueIdentifier)
	collection := db.Database("processdata").Collection("moneothingwithrawdataextended")
	filter := bson.D{{"uniqueidentifier", bson.Regex{Pattern: regex, Options: "i"}}, {"thingid", body.ThingId}}

	log.Println("Executing query")
	cur, err := collection.Find(context.TODO(), filter, pageOptions)
	log.Println("Executed query")
	var results = []moneothingwithvalue{}
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
	
	defer cur.Close(context.Background())

    c.IndentedJSON(http.StatusOK, results)

	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingByThingAndUnique at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())

}
func getMoneoThingWithValue(c *gin.Context){
	var body valuesearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
     now := time.Now()
	log.Printf("----> Starting getMoneoThingWithValue by thing at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	pageOptions := options.Find()
 	pageOptions.SetSkip(int64(body.PageNumber * body.PageSize)) //0-i
 	pageOptions.SetLimit(int64(body.PageSize))

	regex := fmt.Sprintf("^%s", body.Value)
	collection := db.Database("processdata").Collection("moneothingwithrawdataextended")
	filter := bson.D{{"value", bson.Regex{Pattern: regex, Options: "i"}}}

	log.Println("Executing query")
	cur, err := collection.Find(context.TODO(), filter, pageOptions)
	log.Println("Executed query")
	var results = []moneothingwithvalue{}
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
	
	defer cur.Close(context.Background())
    c.IndentedJSON(http.StatusOK, results)

	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingWithValue at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur)

}

func connectDB() (*mongo.Client, error){

//SetAuth(options.Credential{Username: "richie", Password: "0NolonopA0"}).
client, err := mongo.Connect(options.Client().ApplyURI("mongodb://192.168.66.11:27017"))
if err != nil { return nil,err }
return client, err
}

func main() {
	f, err := os.OpenFile("logfile.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
    	log.Fatalf("error opening file: %v", err)
	}
	
	log.SetOutput(f)
	
	/*db, err := connectDB()
	if err != nil{
		panic(err)
	}
	fmt.Println("Successfully connected to Mongo!")
	
	insertData(db)*/
	
	router := gin.Default()
    router.GET("/moneothings", getMoneoThings)
	router.GET("/rawdatas", getRawData)
	router.POST("/rawdatas", getRawDataByValue)
	router.POST("/moneothingrawdata/timestamp", getMoneoThingRawDataByTimeStamp)
	router.POST("/moneothingrawdata/timerange", getMoneoThingRawDataByTimeStampRange)
	router.POST("/moneothingrawdata/value", getMoneoThingWithValue)
	router.POST("/moneothingrawdata/thing", getMoneoThingByThingAndUnique)
    router.Run("localhost:4242")
}

func insertData(db *mongo.Client){	
	now := time.Now()
	log.Println("----> Starting insertion of data at: ", now)
	collection := db.Database("processdata").Collection("moneothing")

	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil { log.Fatal(err) }
	var results = []moneothing{}
	var moneothingIds []int64
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
	for _, v := range results {
		fmt.Println("Found record all:", v.Id, v.ThingId, v.UniqueIdentifier)
		moneothingIds = append(moneothingIds, v.Id)
	} 
	if(len(moneothingIds) == 0){
		for _, v := range moneothings {
			doc, err := toDoc(v)
			if err != nil{
				panic(err)
			}
			res, err := collection.InsertOne(context.Background(), doc)
			if err != nil {
				panic(err)
			}
		
			fmt.Println("New moneothing record ID is:", res.InsertedID)
			moneothingIds = append(moneothingIds, v.Id)
		} 
	}
	
	
	/*cur, err = collection.Find(context.Background(), bson.D{})
	if err != nil { log.Fatal(err) }
	for cur.Next(context.Background()) {
  // To decode into a struct, use cursor.Decode()
  	result := &moneothing{}
  	err := cur.Decode(result)
  	if err != nil { log.Fatal(err) }
  	fmt.Println("Found record:", result.Id, result.ThingId, result.UniqueIdentifier)
	}*/
	collection = db.Database("processdata").Collection("rawdata")
	cur, err = collection.Find(context.Background(), bson.D{})
	if err != nil { log.Fatal(err) }
	var rawdataresults = []rawdata{}
	var rawdataIds []int64
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &rawdataresults); err != nil {
  		log.Fatal(err)
	}
for _, v := range results {
		fmt.Println("Found record all:", v.Id)
		rawdataIds = append(rawdataIds, v.Id)
	} 
	
	if(len(rawdataIds) == 0){
		for i := 0; i < 100; i++{
			var rawdata = rawdata{
				Id: int64(i),
				Value: strconv.FormatFloat(randFloat(-10.00, 40.00), 'f', -1, 64),
			}
			doc, err := toDoc(rawdata)
			if err != nil {
				panic(err)
			}
			res, err := collection.InsertOne(context.Background(), doc)
			if err != nil {
				panic(err)
			}
			
			fmt.Println("New rawdata record ID is:", res.InsertedID)
			rawdataIds = append(rawdataIds, rawdata.Id)
		}
	}
	
	collection = db.Database("processdata").Collection("moneothingrawdata")
	actualCount, err := collection.CountDocuments(context.Background(), bson.D{})
	if err != nil {
     panic(err)
	}
	for i := actualCount + 1; i < 5000000; i++{
		var moneothingrawdata = moneothingrawdata{
			Id: int64(i),
			ThingId: moneothingIds[i%3],
			RawDataId: rand.Int63n(100),
			TimeStamp: time.Now().UTC().UnixMilli(),
		}
     	doc, err := toDoc(moneothingrawdata)
		res, err := collection.InsertOne(context.Background(), doc)
		if err != nil {
			panic(err)
		}
		
		fmt.Println("New record ID is:", i, res.InsertedID)
	}
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished inserting data at: ", after, dur)
}

func randFloat(min, max float64) float64 {
    res :=  min + rand.Float64() * (max - min)
    return res
}

func toDoc(v interface{}) (doc *bson.D, err error) {
    data, err := bson.Marshal(v)
    if err != nil {
        return
    }

    err = bson.Unmarshal(data, &doc)
    return
}