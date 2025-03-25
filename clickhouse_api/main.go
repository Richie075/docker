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
	log.Println("----> Starting getting moneothingrawdata at: ", now)
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
	log.Println("----> Finished getting moneothings at: ", after, dur)
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
	log.Println("----> Starting getting moneothingrawdata at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}
	
	var moneothing moneothing
	var moneothingrawdatas []moneothingrawdata
	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothing WHERE thingid = '%s' AND uniqueidentifier = '%s'`, body.ThingId, body.UniqueIdentifier)
	rows, err := db.Query(context.Background(),	sqlstatement)
	if err != nil {
	panic(err)
	}
	for rows.Next() {
		err = rows.Scan(&moneothing.Id, &moneothing.ThingId, &moneothing.UniqueIdentifier, &moneothing.DisplayName)
		if(err != nil){
			panic(err)
		}
	}
	rows.Close()

	sqlstatement = fmt.Sprintf(`SELECT * FROM processdata.moneothingrawdata mr INNER JOIN processdata.rawdata r ON mr.rawdataid = r.id WHERE thingid = %d ORDER BY mr.timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY `, moneothing.Id, body.PageNumber * body.PageSize, body.PageSize)
	var moneothingrawdata moneothingrawdata
	var rawdata rawdata
	rows, err = db.Query(context.Background(),	sqlstatement)
	if err != nil {
	panic(err)
	}
	for rows.Next() {
		err = rows.Scan(&moneothingrawdata.Id, &moneothingrawdata.ThingId, &moneothingrawdata.RawDataId, &moneothingrawdata.TimeStamp, &rawdata.Id, &rawdata.Value)
		if(err != nil){
			panic(err)
		}
		moneothingrawdata.Rawdata =	rawdata
		moneothingrawdatas = append(moneothingrawdatas, moneothingrawdata)
		
	}
	rows.Close()
	moneothing.Data = moneothingrawdatas
	c.IndentedJSON(http.StatusCreated, moneothing)
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished getting moneothings at: ", after, dur)
}
func getRawDatas(c *gin.Context) {
	now := time.Now()
	log.Println("----> Starting getting rawdata at: ", now)
	db, err := connectDB()
	if err != nil{
		panic(err)
	}

	rows, err := db.Query(context.Background(),"SELECT * FROM processdata.rawdata")
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
	log.Println("----> Finished getting rawdata at: ", after, dur)
}

func getMoneoThingRawData(c *gin.Context) {
	
}

func getRawDataByValue(c *gin.Context) {
	
}

func getMoneoThingRawDataByTimeStamp(c *gin.Context) {

}
func getMoneoThinWithValueAndTimestamp(c *gin.Context){}

func getMoneoThinWithTimestamp(c *gin.Context){}

func getMoneoThinWithValue(c *gin.Context){}

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
	//db, err := connectDB()
	//if err != nil {
	//panic(err)
	//}
	//fmt.Println("Successfully connected to Clickhouse!")
	//insertData(db, context.Background())
	//defer db.Close()
	

	router := gin.Default()
    router.GET("/moneothings", getMoneoThings)
	router.GET("/rawdata", getRawDatas)
	router.GET("/moneothingrawdata", getMoneoThingRawData)
	
	router.GET("/rawdata/:value", getRawDataByValue)
	router.GET("/moneothingrawdata/:timestamp", getMoneoThingRawDataByTimeStamp)
	router.POST("/moneothingrawdata/thing", getMoneoThingByIdAndUnique)
	router.Run("localhost:4243")
}

func insertData(db driver.Conn, ctx context.Context){
	
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
		fmt.Println("Thing: %d ThningId: %s, Uniqueidentifier: %s, DisplayName: %s", moneothing.Id, moneothing.ThingId.String(), moneothing.UniqueIdentifier, moneothing.DisplayName)
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

	for i := 0; i < 100; i++{
		var rawdata = new(rawdata)
		rawdata.Value = strconv.FormatFloat(randFloat(-10.00, 40.00), 'f', -1, 64)
		insertQuery := fmt.Sprintf(sqlStatement, i+1, rawdata.Value)
		db.QueryRow(ctx, insertQuery)
    	fmt.Println("New record ID is:", int64(i))
		rawDataIds = append(rawDataIds,int64(i) )
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