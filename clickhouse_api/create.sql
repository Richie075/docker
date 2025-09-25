CREATE TABLE IF NOT EXISTS processdata.moneothing
(
    id Int64,
    thingid UUID,
    uniqueidentifier String,
    displayname String
)
ENGINE = MergeTree()
ORDER BY (thingid, uniqueidentifier)


CREATE TABLE IF NOT EXISTS processdata.rawdata
(
    id Int64,
    value String
)
ENGINE = MergeTree()
ORDER BY value

CREATE TABLE IF NOT EXISTS processdata.moneothingrawdata
(
    id Int64,
    thingid Int64,
    rawdataid Int64,
    timestamp DateTime('UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY timestamp

    CREATE VIEW processdata.moneothingwithrawdata
 AS
SELECT
  thingid, 
  uniqueidentifier,
  displayname,
  value, 
  timestamp
FROM (
  SELECT
    m.thingid as thingid, m.uniqueidentifier as uniqueidentifier, m.displayname as displayname, r.value as value, mr.timestamp as timestamp
  FROM processdata.moneothingrawdata mr 
  INNER JOIN processdata.moneothing m ON m.id = mr.thingid
  INNER JOIN processdata.rawdata r ON r.id = mr.rawdataid
)
GROUP BY thingid, uniqueidentifier, displayname, timestamp, value
    
CREATE MATERIALIZED VIEW processdata.moneothingwithrawdata_mv
TO processdata.moneothingwithrawdata AS
SELECT
  thingid, 
  uniqueidentifier,
  displayname,
  value, 
  timestamp
FROM (
  SELECT
    m.thingid as thingid, m.uniqueidentifier as uniqueidentifier, m.displayname as displayname, r.value as value, mr.timestamp as timestamp
  FROM processdata.moneothingrawdata mr 
  INNER JOIN processdata.moneothing m ON m.id = mr.thingid
  INNER JOIN processdata.rawdata r ON r.id = mr.rawdataid
)
GROUP BY thingid, uniqueidentifier, displayname, timestamp, value


CREATE TABLE processdata.moneothingwithrawdata (
  thingid UUID, 
  uniqueidentifier String,
  displayname String,
  value String, 
  timestamp DateTime('UTC')
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(timestamp) 
ORDER BY (thingid, uniqueidentifier, value, timestamp)
