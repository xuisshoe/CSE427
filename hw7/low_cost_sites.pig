-- TODO (A): Replace 'FIXME' to load the test_ad_data.txt file.

-- data = LOAD 'test_ad_data.txt'
-- data = LOAD '/dualcore/ad_data1'
data = LOAD '/dualcore/ad_data1' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);


-- TODO (B): Include only records where was_clicked has a value of 1
wasClick = FILTER data BY was_clicked == 1;

-- TODO (C): Group the data by the appropriate field
grpBySite = GROUP wasClick BY display_site;

/* TODO (D): Create a new relation which includes only the 
 *           display site and the total cost of all clicks 
 *           on that site
 */
newRel = FOREACH grpBySite GENERATE group, SUM(wasClick.cpc) as cost;

-- TODO (E): Sort that new relation by cost (ascending)
sortCost = ORDER newRel BY cost;

-- TODO (F): Display just the first four records to the screen
topFour = LIMIT sortCost 4;

--DUMP topFour;
STORE topFour INTO '/dualcore/topFourLowCost1';
