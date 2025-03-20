package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	dbmodels "postgres_api/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/volatiletech/sqlboiler/boil"
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

var moneothingsDefault = []moneothing{
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
	
	db:= connectDB()
	now := time.Now()
	log.Println("----> Starting getting moneothings at: ", now)
	
	rows, err := db.Query("SELECT * FROM public.moneothing")
	
	if err != nil{
		panic(err)
	}
	//moneothings, err := dbmodels.Moneothings().AllG(ctx)

	var moneothings []moneothing
	var moneothingIds []int64
	for rows.Next() {
		var moneothing moneothing
		err = rows.Scan(&moneothing.Id, &moneothing.ThingId, &moneothing.UniqueIdentifier, &moneothing.DisplayName)
		if(err != nil){
			panic(err)
		}
		moneothings = append(moneothings, moneothing)
		fmt.Println("Thing: %d ThningId: %s, Uniqueidentifier: %s, DisplayName: %s", moneothing.Id, moneothing.ThingId.String(), moneothing.UniqueIdentifier, moneothing.DisplayName)
		moneothingIds = append(moneothingIds, moneothing.Id)
	// Process each row
	}
    c.IndentedJSON(http.StatusOK, moneothings)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting moneothings data at: ", after, dur)
}

func getRawData(c *gin.Context) {
	db:= connectDB()
	now := time.Now()
	log.Println("----> Starting getting rawdata at: ", now)
	
	rows, err := db.Query("SELECT * FROM public.rawdata")
	
	if err != nil{
		panic(err)
	}
	//moneothings, err := dbmodels.Moneothings().AllG(ctx)

	var rawdatas []rawdata
	for rows.Next() {
		var rawdata rawdata
		err = rows.Scan(&rawdata.Id, &rawdata.Value)
		if(err != nil){
			panic(err)
		}
		rawdatas = append(rawdatas, rawdata)
	}
    c.IndentedJSON(http.StatusOK, rawdatas)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting rawdatas data at: ", after, dur)
}

func getMoneoThingRawData(c *gin.Context) {
		db:= connectDB()
	now := time.Now()
	log.Println("----> Starting getting moneothingrawdata at: ", now)
	
	rows, err := db.Query("SELECT * FROM public.moneothingrawdata")
	
	if err != nil{
		panic(err)
	}
	//moneothings, err := dbmodels.Moneothings().AllG(ctx)

	var moneothingrawdatas []moneothingrawdata
	for rows.Next() {
		var moneothingrawdata moneothingrawdata
		err = rows.Scan(&moneothingrawdata.Id, &moneothingrawdata.ThingId, &moneothingrawdata.RawDataId, moneothingrawdata.TimeStamp)
		if(err != nil){
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingrawdata)
	}
    c.IndentedJSON(http.StatusOK, moneothingrawdatas)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting moneothingrawdatas data at: ", after, dur)
}

func getMoneoThingByID(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil{
		panic(err)
	}
    now := time.Now()
	log.Println("----> Starting getting moneothings by id at: ", now)
	db:= connectDB()
	sqlStatement := fmt.Sprintf("SELECT * FROM public.moneothing m INNER JOIN public.moneothingrawdata mr ON mr.thingid = m.id WHERE m.id = '%d'", id)
	rows, err := db.Query(sqlStatement)
	
	if err != nil{
		panic(err)
	}
	//moneothings, err := dbmodels.Moneothings().AllG(ctx)

	var tempthing moneothing
	var moneothing *moneothing
	
	var moneothingrawdata moneothingrawdata
	for rows.Next() {
		err = rows.Scan(&tempthing.Id, &tempthing.ThingId, &tempthing.UniqueIdentifier, &tempthing.DisplayName, &moneothingrawdata.ThingId, &moneothingrawdata.RawDataId, &moneothingrawdata.TimeStamp, &moneothingrawdata.Id,)
		if(err != nil){
			panic(err)
		}
		if moneothing == nil{
			moneothing = &tempthing
		}
		moneothing.Data = append(moneothing.Data, moneothingrawdata)
	
		fmt.Println("Thing: %d ThningId: %s, Uniqueidentifier: %s, DisplayName: %s", moneothing.Id, moneothing.ThingId.String(), moneothing.UniqueIdentifier, moneothing.DisplayName)
		
	}
    c.IndentedJSON(http.StatusOK, moneothing)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting moneothings data at: ", after, dur)
	/*moneothings, err := dbmodels.Moneothings(dbmodels.MoneothingWhere.ID.EQ(id)).OneG(ctx)

	if err != nil{
		panic(err)
	}
    c.IndentedJSON(http.StatusOK, moneothings)
	//db.Disconnect()
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting moneothings data at: ", after, dur)*/
}

func getRawDataByValue(c *gin.Context) {
	/*value := c.Param("value")
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
	log.Println("----> Finished getting rawdata by value data at: ", after, dur)*/
}

func getMoneoThingRawDataByTimeStamp(c *gin.Context) {
	/*layout := "2006-01-02T15:04:05.000Z"
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
	log.Println("----> Finished getting rawdata by value data at: ", after, dur)*/
}


func connectDB() *sql.DB {
	db, err := sql.Open("postgres", "postgres://richie:0NolonopA0@192.168.66.11:5439/processdata?sslmode=disable")
	if err != nil {
		panic(err)
	}
	return db
}

func selectMoneoThingsWithRawData(ctx context.Context, thingID string) {
	moneoThing, err := dbmodels.Moneothings(dbmodels.MoneothingWhere.Thingid.EQ(thingID)).OneG(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Printf("MoneoThing: \n\tID:%d \n\tName:%s \n\tEmail:%s\n", moneoThing.ID, moneoThing.Thingid, moneoThing.Uniqueidentifier)

	rawdata := moneoThing.R.GetThingidMoneothingrawdata()

	for _, a := range rawdata {
		fmt.Printf("RawData: \n\tID:%d \n\tThinId:%d \n\tBody:%s \n\tCreatedAt:%v\n", a.ID, a.Thingid, a.Rawdataid, a.Timestamp)
	}
}

func main() {
	db := connectDB()
	
	boil.SetDB(db)

	//selectMoneoThingsWithRawData(ctx, "380035ab-9190-4c75-a251-fbeb53dc0cb5")
	//insertData()
	//ctx := context.Background()
	/*db := connectDB()

	boil.SetDB(db)

	selectMoneoThingsWithRawData(ctx, "61b5d5ea-7134-4db3-867d-528e79528aae")
	err := db.Ping()
	if err != nil {
	panic(err)
	}
	fmt.Println("Successfully connected to PostgreSQL!")
	defer db.Close()
	*/

	router := gin.Default()
    router.GET("/moneothings", getMoneoThings)
	router.GET("/rawdatas", getRawData)
	router.GET("/moneothingrawdatas", getMoneoThingRawData)
	router.GET("/moneothing/:id", getMoneoThingByID)
	router.GET("/rawdata/:value", getRawDataByValue)
	router.GET("/moneothingrawdata/:timestamp", getMoneoThingRawDataByTimeStamp)
    router.Run("localhost:4241")
}

func insertData(){
	context.Background()
	db, err := sql.Open("postgres", "postgres://richie:0NolonopA0@192.168.66.11:5439/processdata?sslmode=disable")
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
	panic(err)
	}
	fmt.Println("Successfully connected to PostgreSQL!")
	
	f, err := os.OpenFile("logfile.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	now := time.Now()
	log.SetOutput(f)

	log.Println("----> Starting insertion of data at: ", now)
	
	rows, err := db.Query("SELECT * FROM public.moneothing")
	if err != nil {
	panic(err)
	}
	var moneothings []moneothing
	var moneothingIds []int64
	for rows.Next() {
		var moneothing moneothing
		err = rows.Scan(&moneothing.Id, &moneothing.ThingId, &moneothing.UniqueIdentifier, &moneothing.DisplayName)
		if(err != nil){
			panic(err)
		}
		moneothings = append(moneothings, moneothing)
		fmt.Println("Thing: %d ThningId: %s, Uniqueidentifier: %s, DisplayName: %s", moneothing.Id, moneothing.ThingId.String(), moneothing.UniqueIdentifier, moneothing.DisplayName)
		moneothingIds = append(moneothingIds, moneothing.Id)
	// Process each row
	}
	rows.Close()
	rows, err = db.Query("SELECT * FROM public.rawdata")
	if err != nil {
	panic(err)
	}
	var rawDataIds []int64
	for rows.Next() {
		var rawdata rawdata
		err = rows.Scan(&rawdata.Id, &rawdata.Value)
		if(err != nil){
			panic(err)
		}
		rawDataIds = append(rawDataIds, rawdata.Id)
	// Process each row
	}
	rows.Close()
	sqlStatement := `INSERT INTO public.moneothing (thingid, uniqueidentifier, displayname) VALUES ('%s', '%s', '%s')	Returning id`
	var id int64
	
	if(len(moneothings) == 0){
	for i := 0; i < 3; i++{
		insertQuery := fmt.Sprintf(sqlStatement, moneothingsDefault[i].ThingId.String(), moneothingsDefault[i].UniqueIdentifier, moneothingsDefault[i].DisplayName)
		err = db.QueryRow(insertQuery).Scan(&id)
     	if err != nil {
        panic(err)
    	}
    	fmt.Println("New record ID is:", id)
		moneothingIds = append(moneothingIds, id)
	}
	}
	sqlStatement = `INSERT INTO public.rawdata (value) VALUES ('%s')	Returning id`

	if(len(rawDataIds) == 0){
	
	for i := 0; i < 100; i++{
		var rawdata = new(rawdata)
		rawdata.Value = strconv.FormatFloat(randFloat(-10.00, 40.00), 'f', -1, 64)
		insertQuery := fmt.Sprintf(sqlStatement, rawdata.Value)
		err = db.QueryRow(insertQuery).Scan(&id)
     	if err != nil {
        panic(err)
    	}
    	fmt.Println("New record ID is:", id)
		rawDataIds = append(rawDataIds, id)
	}
	}

	sqlStatement = `INSERT INTO public.moneothingrawdata (thingid, rawdataid, timestamp) VALUES ('%d', '%d', '%s')`
	id = 0
	db.Close()
	for i := 0; i < 5000000; i++{
		db, err := sql.Open("postgres", "postgres://richie:0NolonopA0@192.168.66.11:5439/processdata?sslmode=disable")
	if err != nil {
		panic(err)
	}
		insertQuery := fmt.Sprintf(sqlStatement, moneothingIds[i%3], rand.Int63n(100) + 1, time.Now().Format(time.RFC3339))
		db.QueryRow(insertQuery)
    	fmt.Println("Inserted:", i)
		db.Close()
	}
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished inserting data at: ", after, dur)
}

func randFloat(min, max float64) float64 {
    res :=  min + rand.Float64() * (max - min)
    return res
}