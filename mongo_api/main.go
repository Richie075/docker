package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
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

}

type rawdata struct{
	Id int64 `json:"id"`
	Value string `json:"value"`
}

type moneothingrawdata struct{
	Id int64 `json:"id"`
	ThingId int64 `json:"thingid"`
	RawDataId int64 `json:"rawdataid"`
	TimeStamp time.Time `json:"timestamp"`
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
    c.IndentedJSON(http.StatusOK, moneothings)
}

func postMoneoThings(c *gin.Context) {
    var newMoneoThing moneothing

    // Call BindJSON to bind the received JSON to
    // newAlbum.
    if err := c.BindJSON(&newMoneoThing); err != nil {
        return
    }

    // Add the new album to the slice.
    moneothings = append(moneothings, newMoneoThing)
    c.IndentedJSON(http.StatusCreated, newMoneoThing)
}

func getMoneoThingByID(c *gin.Context) {
    id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		panic(err)
	}
    // Loop over the list of albums, looking for
    // an album whose ID value matches the parameter.
    for _, a := range moneothings {
        if a.Id == id {
            c.IndentedJSON(http.StatusOK, a)
            return
        }
    }
    c.IndentedJSON(http.StatusNotFound, gin.H{"message": "moneothing not found"})
}

func connectDB() (*mongo.Client, error){
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()
//SetAuth(options.Credential{Username: "richie", Password: "0NolonopA0"}).
client, err := mongo.Connect( ctx, options.Client().ApplyURI("mongodb://192.168.66.11:27017"))
if err != nil { return nil,err }
return client, err
}

func selectMoneoThingsWithRawData(ctx context.Context, thingID string) {

}

func main() {
	
	db, err := connectDB()
	if err != nil{
		panic(err)
	}
	fmt.Println("Successfully connected to Mongo!")
	
	insertData(db)

	
	

	if err != nil {
	panic(err)
	}
	

	router := gin.Default()
    router.GET("/moneothings", getMoneoThings)
	 router.GET("/albums/:id", getMoneoThingByID)
	router.POST("/albums", postMoneoThings)
    router.Run("localhost:4242")
}

func insertData(db *mongo.Client){
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