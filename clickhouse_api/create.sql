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