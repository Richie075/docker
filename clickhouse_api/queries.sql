SELECT m.thingid, m.uniqueidentifier, m.displayname, mr.timestamp, r.value
FROM processdata.moneothingrawdata AS mr
INNER JOIN processdata.moneothing AS m ON m.id = mr.thingid
INNER JOIN processdata.rawdata AS r ON r.id = mr.rawdataid
WHERE m.id == 1