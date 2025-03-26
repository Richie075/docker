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

69996 , 100, 3
db.stats()
{
  db: 'processdata',
  collections: 4,
  views: 1,
  objects: 3160744,
  avgObjSize: 238.99445636850058,
  dataSize: 755400294,
  storageSize: 109142016,
  indexes: 4,
  indexSize: 51904512,
  totalSize: 161046528, --> 162 MB
  scaleFactor: 1,
  fsUsedSize: 95899766784,
  fsTotalSize: 123453800448,
  ok: 1
}