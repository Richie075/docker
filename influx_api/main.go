package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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

func main() {
	router := gin.Default()
    router.GET("/moneothings", getMoneoThings)
	 router.GET("/albums/:id", getMoneoThingByID)
	router.POST("/albums", postMoneoThings)
    router.Run("localhost:4242")
}