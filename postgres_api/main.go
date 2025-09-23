package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"postgres_api/docs"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/swag/example/celler/httputil"
	"github.com/volatiletech/sqlboiler/boil"
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

type insertrelationdto struct {
	ThingId          uuid.UUID `json:"thingid"`
	UniqueIdentifier string    `json:"uniqueidentifier"`
	Values           []string  `json:"values"`
	Time             time.Time `json:"time"`
	NumberOfDatSets  int       `json:"numberofdatasets"`
	BulkInsert       bool      `json:"bulkinsert"`
}

func insertRelations(c *gin.Context) {
	var body insertrelationdto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Println(fmt.Sprintf("----> Starting insertRelations at: %s, bulkinsert: %t", now.Format(time.RFC3339), body.BulkInsert))
	db := connectDB()

	var values []string
	var rawdataids []int64

	values = append(values, body.Values...)

	numberofdatasets := body.NumberOfDatSets - len(body.Values)
	for i := 0; i < numberofdatasets; i++ {
		str := strconv.FormatFloat(randFloat(-10.00, 40.00), 'f', -1, 64)
		values = append(values, str)
	}
	var searchvalues []string
	for _, v := range values {
		searchvalues = append(searchvalues, fmt.Sprintf(`'%s'`, v))
	}
	join := fmt.Sprintf(`array[%s]`, strings.Join(searchvalues, ","))
	sqlstatement := fmt.Sprintf(`SELECT * FROM public.rawdata WHERE value = ANY(%s)`, join)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)

	if err != nil {
		panic(err)
	}

	var rawdatas []rawdata
	//rawdatastoinsert := values
	for rows.Next() {
		var rawdata rawdata
		err = rows.Scan(&rawdata.Id, &rawdata.Value)
		if err != nil {
			panic(err)
		}
		if index := find(values, rawdata.Value); index != -1 {
			values = append(values[:index], values[index+1:]...)
		}
		rawdatas = append(rawdatas, rawdata)
		rawdataids = append(rawdataids, rawdata.Id)
	}

	var insertvalues []string
	for _, v := range values {
		insertvalues = append(insertvalues, fmt.Sprintf(`('%s')`, v))
	}
	sqlstatement = fmt.Sprintf(`INSERT INTO public.rawdata (value) VALUES %s returning id`, strings.Join(insertvalues, ","))
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err = db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)

	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			panic(err)
		}
		rawdataids = append(rawdataids, id)
	}
	var ids []string
	for _, i := range rawdataids {
		ids = append(ids, strconv.Itoa(int(i)))
	}
	join = fmt.Sprintf(`array[%s]`, strings.Join(ids, ","))
	sqlstatement = fmt.Sprintf(`SELECT * FROM public.rawdata WHERE id = ANY(%s)`, join)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err = db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)

	if err != nil {
		panic(err)
	}

	var rawdatafromdb []rawdata
	//rawdatastoinsert := values
	for rows.Next() {
		var rawdata rawdata
		err = rows.Scan(&rawdata.Id, &rawdata.Value)
		if err != nil {
			panic(err)
		}
		rawdatafromdb = append(rawdatafromdb, rawdata)
	}
	var thingid int64
	sqlstatement = fmt.Sprintf(`SELECT id FROM public.moneothing WHERE thingid = '%s' AND uniqueidentifier = '%s'`, body.ThingId, body.UniqueIdentifier)
	log.Printf("Executing query: %s\n", sqlstatement)
	db.QueryRow(sqlstatement).Scan(&thingid)
	log.Printf("Executed query: %s\n", sqlstatement)
	var insertedids []int64
	if body.BulkInsert {
		var buffer bytes.Buffer
		sqlstatement = `INSERT INTO public.moneothingrawdata (thingid, rawdataid, timestamp) VALUES`
		buffer.WriteString(sqlstatement)
		valuestatement := `('%d', '%d', '%s')`
		starttime := time.Now().Add(time.Duration(-numberofdatasets) * time.Second)

		var insertstring []string
		for _, rd := range rawdatafromdb {
			insertQuery := fmt.Sprintf(valuestatement, thingid, rd.Id, starttime.Format(time.RFC3339))
			insertstring = append(insertstring, insertQuery)
			starttime = starttime.Add(time.Second)
		}
		buffer.WriteString(strings.Join(insertstring, ","))
		buffer.WriteString(` returning id`)
		log.Printf("Executing query: %s\n", buffer.String())
		rows, err = db.Query(buffer.String())
		log.Printf("Executed query: %s\n", buffer.String())
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var insertedid int64
			err = rows.Scan(&insertedid)
			if err != nil {
				panic(err)
			}
			insertedids = append(insertedids, insertedid)
		}
	} else {
		sqlstatement = `INSERT INTO public.moneothingrawdata (thingid, rawdataid, timestamp) VALUES ('%d', '%d', '%s') returning id`
		starttime := time.Now().Add(time.Duration(-numberofdatasets) * time.Second)

		for _, rd := range rawdatafromdb {
			var insertedid int64
			insertQuery := fmt.Sprintf(sqlstatement, thingid, rd.Id, starttime.Format(time.RFC3339))
			log.Printf("Executing query: %s\n", insertQuery)
			starttime = starttime.Add(time.Second)
			err := db.QueryRow(insertQuery).Scan(&insertedid)
			log.Printf("Executed query: %s\n", insertQuery)
			if err != nil {
				panic(err)
			}
			fmt.Println("Inserted:", insertedid)

			insertedids = append(insertedids, insertedid)
		}
	}
	c.IndentedJSON(http.StatusOK, insertedids)
	after := time.Now()
	dur := after.Sub(now)
	log.Println(fmt.Sprintf("----> Finished insertRelations data at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds()))
}

func find(s []string, search string) int {
	for i, v := range s {
		if v == search {
			return i
		}
	}
	return -1
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
	log.Printf("----> Starting getMoneoThings at: %s\n", now.Format(time.RFC3339))
	db := connectDB()

	rows, err := db.Query("SELECT * FROM public.moneothing")

	if err != nil {
		panic(err)
	}

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
	}
	c.IndentedJSON(http.StatusOK, moneothings)

	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThings data at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
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
	db := connectDB()

	sqlstatement := fmt.Sprintf(`SELECT * FROM public.rawdata WHERE value = '%s' ORDER BY value OFFSET %d LIMIT %d`, body.Value, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(sqlstatement)
	log.Printf("Executing query: %s\n", sqlstatement)

	if err != nil {
		httputil.NewError(c, http.StatusNotFound, err)
	}

	var rawdatas []rawdataviewmodel
	for rows.Next() {
		var rawdata rawdataviewmodel
		err = rows.Scan(&rawdata.Id, &rawdata.Value)
		if err != nil {
			httputil.NewError(c, http.StatusNotFound, err)
		}
		rawdatas = append(rawdatas, rawdata)
	}
	c.IndentedJSON(http.StatusOK, rawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getRawDataByValue data at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
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
	log.Printf("----> Starting getMoneoThingByValue at: %s", now.Format(time.RFC3339))
	db := connectDB()

	sqlstatement := fmt.Sprintf(`SELECT * FROM public.moneothingwithrawdata WHERE value = '%s' ORDER BY timestamp OFFSET %d LIMIT %d`, body.Value, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)
	if err != nil {
		panic(err)
	}

	var moneothingrawdatas []moneothingwithvalue
	for rows.Next() {
		var moneothingrawdata moneothingwithvalue
		err = rows.Scan(&moneothingrawdata.ThingId, &moneothingrawdata.UniqueIdentifier, &moneothingrawdata.DisplayName, &moneothingrawdata.Value, &moneothingrawdata.TimeStamp)
		if err != nil {
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingrawdata)
	}
	c.IndentedJSON(http.StatusOK, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingByValue at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

// Get moneothing godoc
// @Summary      get moneothing
// @Description  get moneothing by timestamp
// @Tags         moneothingrawdatas
// @Accept       json
// @Produce      json
// @Param		 timestampsearchdto	body		timestampsearchdto	true	"get moneothing by timestamp"
// @Success      200  {object}  []moneothingwithvalue
// @Router       /moneothingrawdatas/timestamp [post]
func getMoneoThingRawDataByTimeStamp(c *gin.Context) {
	var body timestampsearchdto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getMoneoThingRawDataByTimeStamp at: %s", now.Format(time.RFC3339))
	db := connectDB()

	var operator string
	if body.Lower {
		operator = `<=`
	} else {
		operator = `>=`
	}
	sqlstatement := fmt.Sprintf(`SELECT * FROM public.moneothingwithrawdata WHERE timestamp::timestamptz %s to_timestamp(%d) ORDER BY timestamp OFFSET %d LIMIT %d`, operator, body.Time.UnixMilli()/1000, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)
	if err != nil {
		panic(err)
	}

	var moneothingrawdatas []moneothingwithvalue
	for rows.Next() {
		var moneothingrawdata moneothingwithvalue
		err = rows.Scan(&moneothingrawdata.ThingId, &moneothingrawdata.UniqueIdentifier, &moneothingrawdata.DisplayName, &moneothingrawdata.Value, &moneothingrawdata.TimeStamp)
		if err != nil {
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingrawdata)
	}
	c.IndentedJSON(http.StatusOK, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingRawDataByTimeStamp at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

// Get moneothing godoc
// @Summary      get moneothing with rawdata
// @Description  get moneothing by value
// @Tags         moneothingrawdatas
// @Accept       json
// @Produce      json
// @Param		 timestamprangesearchdto	body		timestamprangesearchdto	true	"get moneothing with rawdata by timerange"
// @Success      200  {object}  []moneothingwithvalue
// @Router       /moneothingrawdatas/timerange [post]
func getMoneoThingRawDataByTimeRange(c *gin.Context) {
	var body timestamprangesearchdto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getMoneoThingRawDataByTimeRange at: %s\n", now.Format(time.RFC3339))
	db := connectDB()

	sqlstatement := fmt.Sprintf(`SELECT * FROM public.moneothingwithrawdata WHERE timestamp::timestamptz >= to_timestamp(%d) AND timestamp::timestamptz <= to_timestamp(%d) ORDER BY timestamp OFFSET %d LIMIT %d`, body.From.UnixMilli()/1000, body.To.UnixMilli()/1000, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)

	if err != nil {
		panic(err)
	}

	var moneothingrawdatas []moneothingwithvalue
	for rows.Next() {
		var moneothingrawdata moneothingwithvalue
		err = rows.Scan(&moneothingrawdata.ThingId, &moneothingrawdata.UniqueIdentifier, &moneothingrawdata.DisplayName, &moneothingrawdata.Value, &moneothingrawdata.TimeStamp)
		if err != nil {
			panic(err)
		}
		moneothingrawdatas = append(moneothingrawdatas, moneothingrawdata)
	}
	c.IndentedJSON(http.StatusOK, moneothingrawdatas)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingRawDataByTimeRange at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
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
func getValuesForMoneoThingDuringTimeRange(c *gin.Context) {
	var body moneothingrawdatatimerangedto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getValuesForMoneoThingDuringTimeRange at: %s", now.Format(time.RFC3339))
	db := connectDB()

	sqlstatement := fmt.Sprintf(`SELECT * FROM public.moneothingwithrawdata WHERE thingid = '%s' AND uniqueidentifier = '%s' AND timestamp::timestamptz >= to_timestamp(%d) AND timestamp::timestamptz <= to_timestamp(%d) ORDER BY timestamp DESC OFFSET %d LIMIT %d`, body.ThingId, body.UniqueIdentifier, body.FromTime.UnixMilli()/1000, body.ToTime.UnixMilli()/1000, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)
	if err != nil {
		panic(err)
	}

	var moneothingwithvaluesviewmodel moneothingwithvaluesviewmodel
	var valuewithtimestampviewmodels []valuewithtimestampviewmodel

	for rows.Next() {
		var valuewithtimestampviewmodel valuewithtimestampviewmodel
		err = rows.Scan(&moneothingwithvaluesviewmodel.ThingId, &moneothingwithvaluesviewmodel.UniqueIdentifier, &moneothingwithvaluesviewmodel.DisplayName, &valuewithtimestampviewmodel.Value, &valuewithtimestampviewmodel.TimeStamp)

		valuewithtimestampviewmodels = append(valuewithtimestampviewmodels, valuewithtimestampviewmodel)
	}

	moneothingwithvaluesviewmodel.Rawdatas = valuewithtimestampviewmodels
	c.IndentedJSON(http.StatusOK, moneothingwithvaluesviewmodel)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getValuesForMoneoThingDuringTimeRange at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
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
func getValuesForMoneoThingAtTime(c *gin.Context) {
	var body moneothingrawdatatimestampdto
	if err := c.BindJSON(&body); err != nil {
		log.Println(err)
	}
	now := time.Now()
	log.Printf("----> Starting getValuesForMoneoThingAtTime at: %s", now.Format(time.RFC3339))
	db := connectDB()

	sqlstatement := fmt.Sprintf(`SELECT * FROM public.moneothingwithrawdata WHERE thingid = '%s' AND uniqueidentifier = '%s' AND timestamp::timestamptz <= to_timestamp(%d) ORDER BY timestamp DESC LIMIT 1`, body.ThingId, body.UniqueIdentifier, body.Time.UnixMilli()/1000)

	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)
	if err != nil {
		panic(err)
	}

	var moneothingwithvaluesviewmodel moneothingwithvaluesviewmodel
	var valuewithtimestampviewmodels []valuewithtimestampviewmodel

	for rows.Next() {
		var valuewithtimestampviewmodel valuewithtimestampviewmodel
		err = rows.Scan(&moneothingwithvaluesviewmodel.ThingId, &moneothingwithvaluesviewmodel.UniqueIdentifier, &moneothingwithvaluesviewmodel.DisplayName, &valuewithtimestampviewmodel.Value, &valuewithtimestampviewmodel.TimeStamp)

		valuewithtimestampviewmodels = append(valuewithtimestampviewmodels, valuewithtimestampviewmodel)
	}

	moneothingwithvaluesviewmodel.Rawdatas = valuewithtimestampviewmodels
	c.IndentedJSON(http.StatusOK, moneothingwithvaluesviewmodel)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getValuesForMoneoThingAtTime at: %s, Duration: %d ms\n", after.Format(time.RFC3339), dur.Milliseconds())
}

// Get moneothing godoc
// @Summary      get moneothing
// @Description  get moneothing
// @Tags         moneothingrawdatas
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
	db := connectDB()

	sqlstatement := fmt.Sprintf(`SELECT * FROM public.moneothingwithrawdata WHERE thingid = '%s' AND uniqueidentifier = '%s' ORDER BY timestamp OFFSET %d LIMIT %d`, body.ThingId, body.UniqueIdentifier, body.PageNumber*body.PageSize, body.PageSize)
	log.Printf("Executing query: %s\n", sqlstatement)
	rows, err := db.Query(sqlstatement)
	log.Printf("Executed query: %s\n", sqlstatement)
	if err != nil {
		panic(err)
	}

	var moneothing moneothing
	for rows.Next() {
		err = rows.Scan(&moneothing.ThingId, &moneothing.UniqueIdentifier, &moneothing.DisplayName)
		if err != nil {
			panic(err)
		}
	}
	c.IndentedJSON(http.StatusOK, moneothing)
	after := time.Now()
	dur := after.Sub(now)
	log.Printf("----> Finished getMoneoThingByIdAndUnique data at: %s, Duration: %d md\n", after.Format(time.RFC3339), dur.Milliseconds())
}

func connectDB() *sql.DB {
	db, err := sql.Open("postgres", "postgres://richie:0NolonopA0@192.168.66.11:5439/processdata?sslmode=disable")
	if err != nil {
		panic(err)
	}
	return db
}

func main() {

	db := connectDB()

	boil.SetDB(db)

	//selectMoneoThingsWithRawData(ctx, "380035ab-9190-4c75-a251-fbeb53dc0cb5")
	insertData()
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

	// programmatically set swagger info
	docs.SwaggerInfo.Title = "Postgresql API"
	docs.SwaggerInfo.Description = "This is a sample server postgres api server."
	docs.SwaggerInfo.Version = "1.0"
	docs.SwaggerInfo.Host = "localhost:4241"
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
			moneothings.POST("", getMoneoThingByIdAndUnique)
		}
		moneothingrawdatas := v1.Group("/moneothingrawdatas")
		{
			moneothingrawdatas.POST("thing", getMoneoThingByIdAndUnique)
			moneothingrawdatas.POST("value", getMoneoThingByValue)
			moneothingrawdatas.POST("timestamp", getMoneoThingRawDataByTimeStamp)
			moneothingrawdatas.POST("timerange", getMoneoThingRawDataByTimeRange)

			moneothingrawdatas.POST("insert", insertRelations)
		}
		moneothingwithrawdatas := v1.Group("/moneothingwithrawdatas")
		{
			moneothingwithrawdatas.POST("timerange", getValuesForMoneoThingDuringTimeRange)
			moneothingwithrawdatas.POST("timestamp", getValuesForMoneoThingAtTime)
		}
	}

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	router.Run(":4241")
}

func insertData() {
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

	f, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
		if err != nil {
			panic(err)
		}
		moneothings = append(moneothings, moneothing)
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
		if err != nil {
			panic(err)
		}
		rawDataIds = append(rawDataIds, rawdata.Id)
		// Process each row
	}
	rows.Close()
	sqlStatement := `INSERT INTO public.moneothing (thingid, uniqueidentifier, displayname) VALUES ('%s', '%s', '%s')	Returning id`
	var id int64

	if len(moneothings) == 0 {
		for i := 0; i < 3; i++ {
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

	if len(rawDataIds) == 0 {

		for i := 0; i < 10000; i++ {
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
	var actualCount int64
	err = db.QueryRow("SELECT COUNT(*) FROM public.moneothingrawdata").Scan(&actualCount)
	if err != nil {
		panic(err)
	}
	upperBound := 5000000 - actualCount
	for i := 0; i < int(upperBound); i++ {

		insertQuery := fmt.Sprintf(sqlStatement, moneothingIds[i%3], rand.Int63n(100)+1, time.Now().Format(time.RFC3339))
		query, err := db.Query(insertQuery)
		if err != nil {
			panic(err)
		}
		fmt.Println("Inserted:", i)
		query.Close()
	}
	after := time.Now()
	dur := after.Sub(now)
	log.Println("----> Finished inserting data at: ", after, dur)
	db.Close()
}

func randFloat(min, max float64) float64 {
	res := min + rand.Float64()*(max-min)
	return res
}
