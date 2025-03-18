package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	dbmodels "postgres_api/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/volatiletech/sqlboiler/v4/boil"
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
	insertData()
	ctx := context.Background()
	db := connectDB()

	boil.SetDB(db)

	selectMoneoThingsWithRawData(ctx, "61b5d5ea-7134-4db3-867d-528e79528aae")
	err := db.Ping()
	if err != nil {
	panic(err)
	}
	fmt.Println("Successfully connected to PostgreSQL!")
	defer db.Close()
	

	router := gin.Default()
    router.GET("/moneothings", getMoneoThings)
	 router.GET("/albums/:id", getMoneoThingByID)
	router.POST("/albums", postMoneoThings)
    router.Run("localhost:4242")
}

func insertData(){
	db, err := sql.Open("postgres", "postgres://richie:0NolonopA0@192.168.66.11:5439/processdata?sslmode=disable")
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
	panic(err)
	}
	fmt.Println("Successfully connected to PostgreSQL!")
	defer db.Close()

	rows, err := db.Query("SELECT * FROM public.moneothings")
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

	sqlStatement := `INSERT INTO public.moneothings (thingid, uniqueidentifier, displayname) VALUES ('%s', '%s', '%s')	Returning id`
	var id int64
	
	if(len(moneothings) == 0){
	for i := 0; i < 3; i++{
		insertQuery := fmt.Sprintf(sqlStatement, moneothings[i].ThingId.String(), moneothings[i].UniqueIdentifier, moneothings[i].DisplayName)
		err = db.QueryRow(insertQuery).Scan(&id)
     	if err != nil {
        panic(err)
    	}
    	fmt.Println("New record ID is:", id)
		moneothingIds = append(moneothingIds, id)
	}
	}
	sqlStatement = `INSERT INTO public.rawdata (value) VALUES ('%s')	Returning id`

	//if(len(rawDataIds) == 0){
	
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
	//}

	sqlStatement = `INSERT INTO public.moneothingrawdata (thingid, rawdataid, timestamp) VALUES ('%d', '%d', '%s')	Returning id`
	id = 0
	for i := 0; i < 5000000; i++{
		insertQuery := fmt.Sprintf(sqlStatement, moneothingIds[i%3], rand.Int63n(102) + 1, time.Now().Format(time.RFC3339))
		err = db.QueryRow(insertQuery).Scan(&id)
     	if err != nil {
        panic(err)
    	}
    	fmt.Println("New record ID is:", id)
	}

}

func randFloat(min, max float64) float64 {
    res :=  min + rand.Float64() * (max - min)
    return res
}