package main

import (
	"bytes"
	"clickhouse_api/docs"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

type moneothing struct {
	Id               int64               `json:"id"`
	ThingId          uuid.UUID           `json:"thingid"`
	UniqueIdentifier string              `json:"uniqueidentifier"`
	DisplayName      string              `json:"displayname"`
	Data             []moneothingrawdata `json:"Data"`
}

type rawdata struct {
	Id    int64               `json:"id"`
	Value string              `json:"value"`
	Data  []moneothingrawdata `json:"Data"`
}

type rawdataviewmodel struct {
	Id    int64  `json:"id"`
	Value string `json:"value"`
}

type moneothingrawdata struct {
	Id         int64      `json:"id"`
	ThingId    int64      `json:"thingid"`
	RawDataId  int64      `json:"rawdataid"`
	TimeStamp  time.Time  `json:"timestamp"`
	Rawdata    rawdata    `json:"rawdata"`
	MoneoThing moneothing `json:"moneothing"`
}

type moneothingwithvaluesviewmodel struct {
	ThingId          uuid.UUID                     `json:"thingid"`
	UniqueIdentifier string                        `json:"uniqueidentifier"`
	DisplayName      string                        `json:"displayname"`
	Rawdatas         []valuewithtimestampviewmodel `json:"rawdatas"`
}

type valuewithtimestampviewmodel struct {
	Value     string    `json:"value"`
	TimeStamp time.Time `json:"timestamp"`
}

type moneothingrawdatatimerangedto struct {
	ThingId          uuid.UUID `json:"thingid"`
	UniqueIdentifier string    `json:"uniqueidentifier"`
	FromTime         time.Time `json:"fromtime"`
	ToTime           time.Time `json:"totime"`
	PageNumber       int       `json:"pagenumber"`
	PageSize         int       `json:"pagesize"`
}

type moneothingrawdatatimestampdto struct {
	ThingId          uuid.UUID `json:"thingid"`
	UniqueIdentifier string    `json:"uniqueidentifier"`
	Time             time.Time `json:"time"`
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
	{Id: 1, ThingId: 1, RawDataId: 1, TimeStamp: time.Now()},
	{Id: 2, ThingId: 2, RawDataId: 2, TimeStamp: time.Now().Add(time.Duration(100))},
	{Id: 3, ThingId: 3, RawDataId: 2, TimeStamp: time.Now().Add(time.Duration(200))},
	{Id: 4, ThingId: 3, RawDataId: 2, TimeStamp: time.Now().Add(time.Duration(500))},
}

type moneothingwithvalue struct {
	ThingId          uuid.UUID `json:"thingid"`
	UniqueIdentifier string    `json:"uniqueidentifier"`
	DisplayName      string    `json:"displayname"`
	Value            string    `json:"value"`
	TimeStamp        time.Time `json:"timestamp"`
}

type valuesearchdto struct {
	Value      string `json:"value"`
	PageNumber int    `json:"pagenumber"`
	PageSize   int    `json:"pagesize"`
}

type timestamprangesearchdto struct {
	From       time.Time `json:"from"`
	To         time.Time `json:"to"`
	PageNumber int       `json:"pagenumber"`
	PageSize   int       `json:"pagesize"`
}

type timestampsearchdto struct {
	Time       time.Time `json:"time"`
	Lower      bool      `json:"lower"`
	PageNumber int       `json:"pagenumber"`
	PageSize   int       `json:"pagesize"`
}

type moneothingsearchdto struct {
	ThingId          uuid.UUID `json:"thingid"`
	UniqueIdentifier string    `json:"uniqueidentifier"`
	PageNumber       int       `json:"pagenumber"`
	PageSize         int       `json:"pagesize"`
}

// Moneothings godoc
// @Summary      Get all moneothings
// @Description  get all moneothings
// @Tags         moneothings
// @Accept       json
// @Produce      json
// @Success      200  {object}  []moneothing
// @Router       /moneothings/all [get]
func getMoneoThings(c *gin.Context) {
	now := time.Now()
	log.Printf("----> Starting getMoneoThings at: %s", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil {
		panic(err)
	}

	log.Println("----> Starting getmoneothings of data at: ", now)
	rows, err := db.Query(context.Background(), "SELECT * FROM processdata.moneothing")
	if err != nil {
		panic(err)
	}

	var moneothings []moneothing
	for rows.Next() {
		var moneothing moneothing
		err = rows.Scan(&moneothing.Id, &moneothing.ThingId, &moneothing.UniqueIdentifier, &moneothing.DisplayName)
		if err != nil {
			panic(err)
		}
		moneothings = append(moneothings, moneothing)
	}
	c.IndentedJSON(http.StatusCreated, moneothings)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThings at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

// Get moneothing godoc
// @Summary      get moneothing
// @Description  get moneothing
// @Tags         moneothings
// @Accept       json
// @Produce      json
// @Param		 moneothingsearchdto	body		moneothingsearchdto	true	"get a moneothing"
// @Success      200  {object}  moneothing
// @Router       /moneothings [post]
func getMoneoThingByIdAndUnique(c *gin.Context) {
	var body moneothingsearchdto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getMoneoThingByIdAndUnique at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil {
		panic(err)
	}

	var moneothingrawdatas []moneothingwithvalue

	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothingwithrawdata WHERE thingid = '%s' AND uniqueidentifier = '%s' ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, body.ThingId, body.UniqueIdentifier, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(context.Background(), sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)

	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var moneothingwithvalue moneothingwithvalue
		err = rows.Scan(&moneothingwithvalue.ThingId, &moneothingwithvalue.UniqueIdentifier, &moneothingwithvalue.DisplayName, &moneothingwithvalue.Value, &moneothingwithvalue.TimeStamp)
		if err != nil {
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingwithvalue)
	}
	rows.Close()

	c.IndentedJSON(http.StatusCreated, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingByIdAndUnique at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

// Get moneothing godoc
// @Summary      get moneothing
// @Description  get moneothing by value
// @Tags         moneothingrawdatas
// @Accept       json
// @Produce      json
// @Param		 valuesearchdto	body		valuesearchdto	true	"get moneothing by value"
// @Success      200  {object}  []moneothingwithvalue
// @Router       /moneothingrawdatas/value [post]
func getMoneoThingByValue(c *gin.Context) {
	var body valuesearchdto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getMoneoThingByValue at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil {
		panic(err)
	}

	var moneothingrawdatas []moneothingwithvalue

	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothingwithrawdata WHERE value = '%s' ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, body.Value, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(context.Background(), sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)

	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var moneothingwithvalue moneothingwithvalue
		err = rows.Scan(&moneothingwithvalue.ThingId, &moneothingwithvalue.UniqueIdentifier, &moneothingwithvalue.DisplayName, &moneothingwithvalue.Value, &moneothingwithvalue.TimeStamp)
		if err != nil {
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingwithvalue)
	}
	rows.Close()

	c.IndentedJSON(http.StatusCreated, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingByValue at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

// ShowRawdata godoc
// @Summary      Show rawdatas
// @Description  get rawdatas by value
// @Tags         rawdatas
// @Accept       json
// @Produce      json
// @Param		 valuesearchdto	body		valuesearchdto	true	"Get rawdatas"
// @Success      200  {object}  []rawdataviewmodel
// @Router       /rawdatas [post]
func getRawDataByValue(c *gin.Context) {
	var body valuesearchdto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getRawDataByValue at: %s\n", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}

	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.rawdata WHERE value = '%s' ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, body.Value, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(context.Background(), sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)
	if err != nil {
		panic(err)
	}

	var rawdatas []rawdata
	for rows.Next() {
		var rawdata rawdata
		err = rows.Scan(&rawdata.Id, &rawdata.Value)
		if err != nil {
			panic(err)
		}
		rawdatas = append(rawdatas, rawdata)
	}
	c.IndentedJSON(http.StatusCreated, rawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getRawDataByValue at: %s, Duration: %d ms\n", after, dur)
}

// Get values for moneothing godoc
// @Summary      get values for moneothing at timestamp
// @Description  get values for moneothing
// @Tags         moneothingwitrawdatas
// @Accept       json
// @Produce      json
// @Param		 moneothingrawdatatimestampdto	body		moneothingrawdatatimestampdto	true	"Rawdata next to timerange"
// @Success      200  {object}  moneothingwithvaluesviewmodel
// @Router       /moneothingwithrawdatas/timestamp [post]
func getMoneoThingRawDataByTimeStamp(c *gin.Context) {
	var body moneothingrawdatatimestampdto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getMoneoThingRawDataByTimeStamp at: %s", now.Format(time.RFC3339))
	db, err := connectDB()
	if err != nil {
		panic(err)
	}

	var moneothingwithvaluesviewmodel moneothingwithvaluesviewmodel
	var valuewithtimestampviewmodels []valuewithtimestampviewmodel

	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothingwithrawdata WHERE  thingid = '%s' AND uniqueidentifier = '%s' timestamp <= parseDateTimeBestEffort('%s') ORDER BY timestamp OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY`, body.ThingId, body.UniqueIdentifier, body.Time)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(context.Background(), sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)

	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var valuewithtimestampviewmodel valuewithtimestampviewmodel
		err = rows.Scan(&moneothingwithvaluesviewmodel.ThingId, &moneothingwithvaluesviewmodel.UniqueIdentifier, &moneothingwithvaluesviewmodel.DisplayName, &valuewithtimestampviewmodel.Value, &valuewithtimestampviewmodel.TimeStamp)
		if err != nil {
			panic(err)
		}
		valuewithtimestampviewmodels = append(valuewithtimestampviewmodels, valuewithtimestampviewmodel)
	}
	moneothingwithvaluesviewmodel.Rawdatas = valuewithtimestampviewmodels
	rows.Close()

	c.IndentedJSON(http.StatusCreated, moneothingwithvaluesviewmodel)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingRawDataByTimeStamp at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

// Get values for moneothing godoc
// @Summary      get values for moneothing in given timerange
// @Description  get values for moneothing
// @Tags         moneothingwitrawdatas
// @Accept       json
// @Produce      json
// @Param		 moneothingrawdatatimerangedto	body		moneothingrawdatatimerangedto	true	"Rawdatas for timerange"
// @Success      200  {object}  moneothingwithvaluesviewmodel
// @Router       /moneothingwithrawdatas/timerange [post]
func getMoneoThingRawDataByTimeRange(c *gin.Context) {
	var body moneothingrawdatatimerangedto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getMoneoThingRawDataByTimeRange at: %s\n", now.Format(time.DateTime))
	db, err := connectDB()
	if err != nil {
		panic(err)
	}

	var moneothingwithvaluesviewmodel moneothingwithvaluesviewmodel
	var valuewithtimestampviewmodels []valuewithtimestampviewmodel

	sqlstatement := fmt.Sprintf(`SELECT * FROM processdata.moneothingwithrawdata WHERE thingid = '%s' AND uniqueidentifier = '%s' AND timestamp >= toDateTime('%s') AND timestamp <= toDateTime('%s') ORDER BY timestamp OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, body.ThingId, body.UniqueIdentifier, body.FromTime.Format(time.DateTime), body.ToTime.Format(time.DateTime), body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(context.Background(), sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)

	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var valuewithtimestampviewmodel valuewithtimestampviewmodel
		err = rows.Scan(&moneothingwithvaluesviewmodel.ThingId, &moneothingwithvaluesviewmodel.UniqueIdentifier, &moneothingwithvaluesviewmodel.DisplayName, &valuewithtimestampviewmodel.Value, &valuewithtimestampviewmodel.TimeStamp)
		if err != nil {
			panic(err)
		}
		valuewithtimestampviewmodels = append(valuewithtimestampviewmodels, valuewithtimestampviewmodel)
	}
	rows.Close()

	moneothingwithvaluesviewmodel.Rawdatas = valuewithtimestampviewmodels
	c.IndentedJSON(http.StatusCreated, moneothingwithvaluesviewmodel)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingRawDataByTimeRange at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
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
	f, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
	insertData(true)
	//db, err := connectDB()
	//if err != nil {
	//panic(err)
	//}
	//fmt.Println("Successfully connected to Clickhouse!")
	//insertData(db, context.Background())
	//defer db.Close()

	docs.SwaggerInfo.Title = "Clickhouse API"
	docs.SwaggerInfo.Description = "This is a sample server clickhouse api server."
	docs.SwaggerInfo.Version = "1.0"
	docs.SwaggerInfo.Host = "localhost:4243"
	docs.SwaggerInfo.BasePath = "/api/v1"
	docs.SwaggerInfo.Schemes = []string{"http", "https"}

	router := gin.Default()

	v1 := router.Group("/api/v1")
	{
		rawdatas := v1.Group("/rawdatas")
		{
			rawdatas.POST("", getRawDataByValue)
		}
		moneothings := v1.Group("/moneothings")
		{
			moneothings.GET("all", getMoneoThings)
			//moneothings.POST("", getMoneoThingByIdAndUnique)
		}
		moneothingrawdatas := v1.Group("/moneothingwithrawdatas")
		{
			moneothingrawdatas.POST("thing", getMoneoThingByIdAndUnique)
			moneothingrawdatas.POST("value", getMoneoThingByValue)
			moneothingrawdatas.POST("timestamp", getMoneoThingRawDataByTimeStamp)
			moneothingrawdatas.POST("timerange", getMoneoThingRawDataByTimeRange)

			//moneothingrawdatas.POST("insert", insertRelations)
		}
	}

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	router.Run(":4243")
}

func insertData(bulk bool) {
	db, err := connectDB()
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
		if err != nil {
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
	var rawdataids []int64
	var rawDatasFromDb []rawdata
	for rows.Next() {
		var rawdata rawdata
		err = rows.Scan(&rawdata.Id, &rawdata.Value)
		if err != nil {
			panic(err)
		}
		rawDatasFromDb = append(rawDatasFromDb, rawdata)
		rawdataids = append(rawdataids, rawdata.Id)
		// Process each row
	}

	sqlStatement := `INSERT INTO processdata.moneothing (id, thingid, uniqueidentifier, displayname) VALUES ('%d','%s', '%s', '%s')`

	if len(moneothings) == 0 {
		for i := 0; i < 3; i++ {
			insertQuery := fmt.Sprintf(sqlStatement, moneothingsDefault[i].Id, moneothingsDefault[i].ThingId.String(), moneothingsDefault[i].UniqueIdentifier, moneothingsDefault[i].DisplayName)
			db.QueryRow(ctx, insertQuery)
			moneothingIds = append(moneothingIds, int64(i))
		}
	}
	sqlStatement = `INSERT INTO processdata.rawdata (id, value) VALUES ('%d','%s')`
	if len(rawDatasFromDb) == 0 {
		for i := 0; i < 1000; i++ {
			var rawdata = new(rawdata)
			rawdata.Value = strconv.FormatFloat(randFloat(-10.00, 40.00), 'f', -1, 64)
			insertQuery := fmt.Sprintf(sqlStatement, i+1, rawdata.Value)
			db.QueryRow(ctx, insertQuery)
			rawdataids = append(rawdataids, int64(i))
		}
	}
	if bulk {
		sqlStatement = `INSERT INTO processdata.moneothingrawdata (id,thingid, rawdataid, timestamp) VALUES`
		valuestatement := `(%d,%d,%d, '%s')`
		var actualCount int64
		var insertstring []string
		var buffer bytes.Buffer
		buffer.WriteString(sqlStatement)
		db.QueryRow(ctx, "SELECT id FROM processdata.moneothingrawdata ORDER BY id DESC LIMIT 1").Scan(&actualCount)

		starttime := time.Now().Add(time.Duration(-5000000) * time.Second)
		for i := actualCount + 1; i < 5000001; i++ {
			insertQuery := fmt.Sprintf(valuestatement, i, moneothingIds[i%3], rand.Int63n(1000)+1, starttime.Format(time.DateTime))
			insertstring = append(insertstring, insertQuery)
			starttime = starttime.Add(time.Second)
			if i%10000 == 0 {
				buffer.WriteString(strings.Join(insertstring, ","))
				db.QueryRow(ctx, buffer.String())
				insertstring = nil
				buffer.Reset()
				buffer.WriteString(sqlStatement)
			}
		}
	} else {
		sqlStatement = `INSERT INTO processdata.moneothingrawdata (id,thingid, rawdataid, timestamp) VALUES (%d,%d, %d, %d)`
		var actualCount int64
		db.QueryRow(ctx, "SELECT id FROM processdata.moneothingrawdata ORDER BY id DESC LIMIT 1").Scan(&actualCount)
		for i := actualCount + 1; i < 5000000; i++ {
			insertQuery := fmt.Sprintf(sqlStatement, i, moneothingIds[i%3], rand.Int63n(100)+1, time.Now().UnixMilli())
			db.QueryRow(ctx, insertQuery)
			fmt.Println("New record ID is:", i)
		}
	}
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished inserting data at: ", after, dur)
}

func randFloat(min, max float64) float64 {
	res := min + rand.Float64()*(max-min)
	return res
}
