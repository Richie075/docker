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

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
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
	TimeStamp time.Time `json:"timestamp"`
	Rawdata rawdata `json:"rawdata"`
	MoneoThing moneothing `json:"moneothing"`
}

type moneothingwithvalue struct{
	ThingId uuid.UUID `json:"thingid"`
	UniqueIdentifier string  `json:"uniqueidentifier"`
	DisplayName string `json:"displayname"`
	Value string `json:"value"`
}

type valuesearchdto struct{
	Value string `json:"value"`
}

type timestamprangesearchdto struct{
	From time.Time `json:"from"`
	To time.Time `json:"to"`
}

type timestampsearchdto struct{
	Time time.Time `json:"time"`
	Lower bool `json:"lower"`
}

type moneothingsearchdto struct{
	ThingId uuid.UUID `json:"thingid"`
	UniqueIdentifier string  `json:"uniqueidentifier"`
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

var moneothingrawdatas = []moneothingrawdata{
	{Id:1, ThingId: 1, RawDataId: 1, TimeStamp: time.Now()},
	{Id:2, ThingId: 2, RawDataId: 2, TimeStamp: time.Now().Add(time.Duration(100))},
	{Id:3, ThingId: 3, RawDataId: 2, TimeStamp: time.Now().Add(time.Duration(200))},
	{Id:4, ThingId: 3, RawDataId: 2, TimeStamp: time.Now().Add(time.Duration(500))},
}

func getMoneoThings(c *gin.Context) {
	now := time.Now()
	log.Println("----> Starting getting moneothings at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothing")

	cur, err := collection.Find(context.Background(), bson.D{}, &options.FindOptions{})
	var results = []moneothing{}
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
    c.IndentedJSON(http.StatusOK, results)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting moneothings data at: ", after, dur)
}

func getRawData(c *gin.Context) {
	now := time.Now()
	log.Println("----> Starting getting rawdata at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("rawdata")

	cur, err := collection.Find(context.Background(), bson.D{}, &options.FindOptions{})
	var results = []rawdata{}
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
    c.IndentedJSON(http.StatusOK, results)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting rawdata at: ", after, dur)
}

func getMoneoThingRawData(c *gin.Context) {
	now := time.Now()
	log.Println("----> Starting getting moneothingrawdata at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothingrawdata")

	cur, err := collection.Find(context.Background(), bson.D{}, &options.FindOptions{})
	var results = []moneothingrawdata{}
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}
    c.IndentedJSON(http.StatusOK, results)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting rawdata at: ", after, dur)
}

func getMoneoThingByID(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
    now := time.Now()
	log.Println("----> Starting getting moneothings by id at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothing")

	var result = moneothing{}
	
	collection.FindOne(context.TODO(), bson.M{"id": id}).Decode(&result)
	
	var data = []moneothingrawdata{}
	collection = db.Database("processdata").Collection("moneothingrawdata")
	batchSize := int32(100)
	cur, err := collection.Find(context.Background(), bson.M{"thingid": result.Id}, &options.FindOptions{BatchSize: &batchSize})
	if err != nil { log.Fatal(err) }
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &data); err != nil {
  		log.Fatal(err)
	}
	result.Data = data
    c.IndentedJSON(http.StatusOK, result)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting moneothing by id data at: ", after, dur)
}

func getRawDataByValue(c *gin.Context) {
	value := c.Param("value")
    now := time.Now()
	log.Println("----> Starting getting rawdata by value at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("rawdata")

	var result = rawdata{}
	
	collection.FindOne(context.TODO(), bson.M{"value": value}).Decode(&result)
	
    c.IndentedJSON(http.StatusOK, result)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting rawdata by value data at: ", after, dur)
}

func getMoneoThingRawDataByTimeStamp(c *gin.Context) {
	layout := "2006-01-02T15:04:05.000Z"
	str := c.Param("timestamp")
	timestamp, err := time.Parse(layout, str)
    now := time.Now()
	log.Println("----> Starting getting rawdata by value at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothingrawdata")

	var result = moneothingrawdata{}
	
	collection.FindOne(context.TODO(), bson.M{"timestamp": timestamp}).Decode(&result)
	
	var thing = moneothing{}
	collection = db.Database("processdata").Collection("moneothing")
	collection.FindOne(context.TODO(), bson.M{"thinidgid": result.ThingId}).Decode(&thing)

	result.MoneoThing = thing
	var rawdata = rawdata{}
	collection = db.Database("processdata").Collection("rawdata")
	collection.FindOne(context.TODO(), bson.M{"id": result.RawDataId},).Decode(&rawdata)
	
	result.Rawdata = rawdata
    c.IndentedJSON(http.StatusOK, result)

	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting rawdata by value data at: ", after, dur)
}

func getMoneoThingWithTimestamp(c *gin.Context){}

func getMoneoThingByThingAndUnique(c *gin.Context){
	var body moneothingsearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    now := time.Now()
	log.Println("----> Starting getting getMoneoThingWithValue by value at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothing")

	regexunique := fmt.Sprintf("/%s*/", body.UniqueIdentifier) 
	cur, err := collection.Find(context.TODO(), bson.M{"thingid": body.ThingId, "uniquidentifier": bson.M{"$regex": regexunique, "$options": ""}}, &options.FindOptions{})
	var results = []moneothingwithvalue{}
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}	

    c.IndentedJSON(http.StatusOK, results)

	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting getMoneoThingWithValue by value data at: ", after, dur)

}
func getMoneoThingWithValue(c *gin.Context){
	var body valuesearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    now := time.Now()
	log.Println("----> Starting getting getMoneoThingWithValue by value at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	collection := db.Database("processdata").Collection("moneothingwithvalue")

	regex := fmt.Sprintf("/%s*/", body.Value) 
	cur, err := collection.Find(context.TODO(), bson.M{"value": bson.M{"$regex": regex, "$options": ""}}, &options.FindOptions{})
	var results = []moneothingwithvalue{}
	
	defer cur.Close(context.Background())
	if err = cur.All(context.Background(), &results); err != nil {
  		log.Fatal(err)
	}	

    c.IndentedJSON(http.StatusOK, results)

	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting getMoneoThingWithValue by value data at: ", after, dur)

}
func getMoneoThingWithValueAndTimestamp(c *gin.Context){}

func connectDB() (*mongo.Client, error){
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()
//SetAuth(options.Credential{Username: "richie", Password: "0NolonopA0"}).
client, err := mongo.Connect( ctx, options.Client().ApplyURI("mongodb://192.168.66.11:27017"))
if err != nil { return nil,err }
return client, err
}

func main() {
	
	/*db, err := connectDB()
	if err != nil{
		panic(err)
	}
	fmt.Println("Successfully connected to Mongo!")
	
	insertData(db)

	
	

	if err != nil {
	panic(err)
	}
	*/

	router := gin.Default()
    router.GET("/moneothings", getMoneoThings)
	router.GET("/rawdata", getRawData)
	router.GET("/moneothingrawdata", getMoneoThingRawData)
	router.GET("/moneothing/:id", getMoneoThingByID)
	router.GET("/rawdata/:value", getRawDataByValue)
	router.GET("/moneothingrawdata/:timestamp", getMoneoThingRawDataByTimeStamp)
	router.POST("/moneothingwithvalue", getMoneoThingWithValue)
	router.POST("/moneothing", getMoneoThingByThingAndUnique)
    router.Run("localhost:4242")
}

func insertData(db *mongo.Client){
	f, err := os.OpenFile("logfile.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	now := time.Now()
	
	if err != nil {
    	log.Fatalf("error opening file: %v", err)
	}
	
	log.SetOutput(f)

	log.Println("----> Starting insertion of data at: ", now)
	collection := db.Database("processdata").Collection("moneothing")

	cur, err := collection.Find(context.Background(), bson.D{}, &options.FindOptions{})
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
	var rawDataIds []int64
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
		rawDataIds = append(rawDataIds, rawdata.Id)
	}
	//}
	
	collection = db.Database("processdata").Collection("moneothingrawdata")
	for i := 0; i < 5000000; i++{
		var moneothingrawdata = moneothingrawdata{
			Id: int64(i),
			ThingId: moneothingIds[i%3],
			RawDataId: rand.Int63n(100),
			TimeStamp: time.Now(),
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