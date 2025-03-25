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

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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
func getMoneoThings(c *gin.Context) {
	now := time.Now()
	log.Println("----> Starting getMoneoThings at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	rows, err := db.Query(context.Background(),"SELECT * FROM processdata.moneothing")
	if err != nil {
	panic(err)
	}

	log.Println("----> Starting getmoneothings of data at: ", now)
	var moneothings []moneothing
	for rows.Next() {
		var moneothing moneothing
		err = rows.Scan(&moneothing.Id, &moneothing.ThingId, &moneothing.UniqueIdentifier, &moneothing.DisplayName)
		if(err != nil){
			panic(err)
		}
		moneothings = append(moneothings, moneothing)
	}
	c.IndentedJSON(http.StatusCreated, moneothings)
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getMoneoThings at: ", after, dur)
}

func postMoneoThings(c *gin.Context) {
    var newMoneoThing moneothing

    // Call BindJSON to bind the received JSON to
    // newAlbum.
    if err := c.BindJSON(&newMoneoThing); err != nil {
        return
    }

    // Add the new album to the slice.
    moneothingsDefault = append(moneothingsDefault, newMoneoThing)
    c.IndentedJSON(http.StatusCreated, newMoneoThing)
}

func getMoneoThingByIdAndUnique(c *gin.Context) {
	var body moneothingsearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    	now := time.Now()
	log.Println("----> Starting getMoneoThingByIdAndUnique at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}
	
	var moneothingrawdatas []moneothingwithvalue

	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothingwithrawdata WHERE thingid = '%s' AND uniqueidentifier = '%s' ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, body.ThingId, body.UniqueIdentifier, body.PageNumber * body.PageSize, body.PageSize)
	rows, err := db.Query(context.Background(),	sqlstatement)
	
	if err != nil {
	panic(err)
	}
	for rows.Next() {
		var moneothingwithvalue moneothingwithvalue
		err = rows.Scan(&moneothingwithvalue.ThingId, &moneothingwithvalue.UniqueIdentifier, &moneothingwithvalue.DisplayName, &moneothingwithvalue.Value, &moneothingwithvalue.TimeStamp)
		if(err != nil){
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingwithvalue)
	}
	rows.Close()

	
	c.IndentedJSON(http.StatusCreated, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getMoneoThingByIdAndUnique at: ", after, dur)
}

func getMoneoThingByValue(c *gin.Context) {
	var body valuesearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    	now := time.Now()
	log.Println("----> Starting getMoneoThingByValue at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}
	

	var moneothingrawdatas []moneothingwithvalue

	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothingwithrawdata WHERE value = '%s' ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, body.Value, body.PageNumber * body.PageSize, body.PageSize)
	rows, err := db.Query(context.Background(),	sqlstatement)
	
	if err != nil {
	panic(err)
	}
	for rows.Next() {
		var moneothingwithvalue moneothingwithvalue
		err = rows.Scan(&moneothingwithvalue.ThingId, &moneothingwithvalue.UniqueIdentifier, &moneothingwithvalue.DisplayName, &moneothingwithvalue.Value, &moneothingwithvalue.TimeStamp)
		if(err != nil){
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingwithvalue)
	}
	rows.Close()

	
	c.IndentedJSON(http.StatusCreated, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getMoneoThingByValue at: ", after, dur)
}

func getRawDataByValue(c *gin.Context) {
	var body valuesearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    	now := time.Now()
	log.Println("----> Starting getRawDataByValue at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}
	if err != nil{
		panic(err)
	}

	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.rawdata WHERE value = '%s' ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, body.Value, body.PageNumber * body.PageSize, body.PageSize)
	rows, err := db.Query(context.Background(),sqlstatement)
	if err != nil {
	panic(err)
	}

	var rawdatas []rawdata
	for rows.Next() {
		var rawdata rawdata
		err = rows.Scan(&rawdata.Id, &rawdata.Value)
		if(err != nil){
			panic(err)
		}
		rawdatas = append(rawdatas, rawdata)
		

	// Process each row
	}
	c.IndentedJSON(http.StatusCreated, rawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getRawDataByValue at: ", after, dur)
}


func getMoneoThingRawDataByTimeStamp(c *gin.Context) {
	var body timestampsearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    	now := time.Now()
	log.Println("----> Starting getMoneoThingRawDataByTimeStamp at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}
	

	var moneothingrawdatas []moneothingwithvalue
	var operator string 
	if body.Lower{
		operator = `<=`
	}else{
		operator = `>=`
	}
	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothingwithrawdata WHERE timestamp %s parseDateTimeBestEffort('%s') ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, operator, body.Time,body.PageNumber * body.PageSize, body.PageSize)
	rows, err := db.Query(context.Background(),	sqlstatement)
	
	if err != nil {
	panic(err)
	}
	for rows.Next() {
		var moneothingwithvalue moneothingwithvalue
		err = rows.Scan(&moneothingwithvalue.ThingId, &moneothingwithvalue.UniqueIdentifier, &moneothingwithvalue.DisplayName, &moneothingwithvalue.Value, &moneothingwithvalue.TimeStamp)
		if(err != nil){
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingwithvalue)
	}
	rows.Close()

	
	c.IndentedJSON(http.StatusCreated, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getMoneoThingRawDataByTimeStamp at: ", after, dur)
}

func getMoneoThingRawDataByTimeRange(c *gin.Context) {
	var body timestamprangesearchdto
	if err := c.BindJSON(&body); err != nil{
		log.Println(err)
	}
    	now := time.Now()
	log.Println("----> Starting getMoneoThingRawDataByTimeRange at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}
	

	var moneothingrawdatas []moneothingwithvalue
	
	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothingwithrawdata WHERE timestamp >= parseDateTimeBestEffort('%s') AND timestamp <= parseDateTimeBestEffort('%s') ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, body.From, body.To,body.PageNumber * body.PageSize, body.PageSize)
	rows, err := db.Query(context.Background(),	sqlstatement)
	
	if err != nil {
	panic(err)
	}
	for rows.Next() {
		var moneothingwithvalue moneothingwithvalue
		err = rows.Scan(&moneothingwithvalue.ThingId, &moneothingwithvalue.UniqueIdentifier, &moneothingwithvalue.DisplayName, &moneothingwithvalue.Value, &moneothingwithvalue.TimeStamp)
		if(err != nil){
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingwithvalue)
	}
	rows.Close()

	
	c.IndentedJSON(http.StatusCreated, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getMoneoThingRawDataByTimeRange at: ", after, dur)
}

func connectDB() (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"192.168.66.11:8002"},
			Auth: clickhouse.Auth{
				Database: "processdata",
				Username: "richie",
				Password: "0NolonopA0",
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			TLS: nil,
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func selectMoneoThingsWithRawData(ctx context.Context, thingID string) {

}

func main() {
	f, err := os.OpenFile("logfile.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)

	if err != nil {
	panic(err)
	}
	log.SetOutput(f)
	insertData()
	//db, err := connectDB()
	//if err != nil {
	//panic(err)
	//}
	//fmt.Println("Successfully connected to Clickhouse!")
	//insertData(db, context.Background())
	//defer db.Close()
	

	router := gin.Default()
    router.GET("/moneothings", getMoneoThings)
	router.POST("/rawdatas", getRawDataByValue)
	router.POST("/moneothingrawdata/thing", getMoneoThingByIdAndUnique)
	router.POST("/moneothingrawdata/value", getMoneoThingByValue)
	router.POST("/moneothingrawdata/timestamp", getMoneoThingRawDataByTimeStamp)
	router.POST("/moneothingrawdata/timerange", getMoneoThingRawDataByTimeRange)
	router.Run("localhost:4243")
}

func insertData(){
	db,err := connectDB()
	ctx := context.Background()
	now := time.Now()
	rows, err := db.Query(ctx, "SELECT * FROM processdata.moneothing")
	if err != nil {
	panic(err)
	}

	log.Println("----> Starting insertion of data at: ", now)
	var moneothings []moneothing
	var moneothingIds []int64
	for rows.Next() {
		var moneothing moneothing
		err = rows.Scan(&moneothing.Id, &moneothing.ThingId, &moneothing.UniqueIdentifier, &moneothing.DisplayName)
		if(err != nil){
			panic(err)
		}
		moneothings = append(moneothings, moneothing)
		moneothingIds = append(moneothingIds, moneothing.Id)
	// Process each row
	}

	rows, err = db.Query(ctx, "SELECT * FROM processdata.rawdata")
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

	sqlStatement := `INSERT INTO processdata.moneothing (id, thingid, uniqueidentifier, displayname) VALUES ('%d','%s', '%s', '%s')`

	
	if(len(moneothings) == 0){
	for i := 0; i < 3; i++{
		insertQuery := fmt.Sprintf(sqlStatement,  moneothingsDefault[i].Id, moneothingsDefault[i].ThingId.String(), moneothingsDefault[i].UniqueIdentifier, moneothingsDefault[i].DisplayName)
		db.QueryRow(ctx, insertQuery)
     	
    	fmt.Println("New record ID is:", i)
		moneothingIds = append(moneothingIds, int64(i))
	}
	}
	sqlStatement = `INSERT INTO processdata.rawdata (id, value) VALUES ('%d','%s')`
if(len(rawdatas) == 0){
	for i := 0; i < 100; i++{
		var rawdata = new(rawdata)
		rawdata.Value = strconv.FormatFloat(randFloat(-10.00, 40.00), 'f', -1, 64)
		insertQuery := fmt.Sprintf(sqlStatement, i+1, rawdata.Value)
		db.QueryRow(ctx, insertQuery)
    	fmt.Println("New record ID is:", int64(i))
		rawDataIds = append(rawDataIds,int64(i) )
	}
}

	sqlStatement = `INSERT INTO processdata.moneothingrawdata (id,thingid, rawdataid, timestamp) VALUES (%d,%d, %d, %d)`

	for i := 0; i < 5000000; i++{
		insertQuery := fmt.Sprintf(sqlStatement, i, moneothingIds[i%3], rand.Int63n(100) + 1, time.Now().UnixMilli())
		db.QueryRow(ctx, insertQuery)
    	fmt.Println("New record ID is:", i)
	}
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished inserting data at: ", after, dur)
}

func randFloat(min, max float64) float64 {
    res :=  min + rand.Float64() * (max - min)
    return res
}