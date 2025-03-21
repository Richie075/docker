db.createView( "moneothingwithrawdata", "moneothing", [
   {
      $lookup:
         {
            from: "moneothingrawdata",
            localField: "id",
            foreignField: "thingid",
            as: "moneothingdocs"
         }
   },
   { $unwind: "$moneothingdocs" },
   {
      $lookup:
         {
            from: "rawdata",
            localField: "id",
            foreignField: "rawdataid",
            as: "rawdatadocs"
         }
   },
   { $unwind: "$rawdatadocs" },
   {
      $project:
         {
           _id: 0,
           uniqueidentifier: 1,
           displayname: 1,
           timestamp: "$moneothingdocs.timestamp",
		   value: "$rawdatadocs.value"
         }
   }
] )




db.createView( "moneothingwithrawdatasimple", "moneothing", [
   {
      $lookup:
         {
            from: "moneothingrawdata",
            localField: "id",
            foreignField: "thingid",
            as: "moneothingdocs"
         }
   },
   { $unwind: "$moneothingdocs" },
  
   {
      $project:
         {
           _id: 0,
           uniqueidentifier: 1,
           displayname: 1,
           timestamp: "$moneothingdocs.timestamp"
         }
   }
] )

db.createView( "moneothingwithrawdataextended", "moneothingrawdata", [
   {
      $lookup:
         {
            from: "moneothing",
            localField: "thingid",
            foreignField: "id",
            as: "moneothingdocs"
         }
   },
   { $unwind: "$moneothingdocs" },
   {
  $lookup:
         {
            from: "rawdata",
            localField: "rawdataid",
            foreignField: "id",
            as: "rawdatadocs"
         }
   },
   { $unwind: "$rawdatadocs" },
  
   {
      $project:
         {
           _id: 0,
		   thingid: "$moneothingdocs.thingid",
           uniqueidentifier: "$moneothingdocs.uniqueidentifier",
           displayname: "$moneothingdocs.displayname",
           timestamp: 1,
		   value: "$rawdatadocs.value"
         }
   }
] )

db.createView( "moneothingwithvalue", "moneothingrawdata", [
   {
      $lookup:
         {
            from: "moneothing",
            localField: "thingid",
            foreignField: "id",
            as: "moneothingdocs"
         }
   },
   { $unwind: "$moneothingdocs" },
   {
  $lookup:
         {
            from: "rawdata",
            localField: "rawdataid",
            foreignField: "id",
            as: "rawdatadocs"
         }
   },
   { $unwind: "$rawdatadocs" },
  
   {
      $project:
         {
           _id: 0,
		   thingid: "$moneothingdocs.thingid",
           uniqueidentifier: "$moneothingdocs.uniqueidentifier",
           displayname: "$moneothingdocs.displayname",
		   value: "$rawdatadocs.value"
         }
   }
] )

db.createView( "moneothingwithtimestamp", "moneothingrawdata", [
   {
      $lookup:
         {
            from: "moneothing",
            localField: "thingid",
            foreignField: "id",
            as: "moneothingdocs"
         }
   },
   { $unwind: "$moneothingdocs" },
  
   {
      $project:
         {
           _id: 0,
		   thingid: "$moneothingdocs.thingid",
           uniqueidentifier: "$moneothingdocs.uniqueidentifier",
           displayname: "$moneothingdocs.displayname",
		   timestamp: 1
         }
   }
] )