-- TODO (A): Replace 'FIXME' to load the test_ad_data.txt file.

-- data = LOAD 'test_ad_data.txt'
-- data = LOAD '/dualcore/ad_data1'
data = LOAD '/dualcore/ad_data1' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);


-- TODO (B): regroup the data by keyword, since the goal is to find high cost based on keyword
keyWord = GROUP data BY keyword; 


-- TODO (C): rearrange the data so that the data is sorted by the sum of the cost of each keyword 
keyWordSum = FOREACH keyWord GENERATE group, SUM(data.cpc) AS number;

-- TODO (D): Sort the new data by cost (desending)
keyWordSumDesc = ORDER keyWordSum BY number DESC;

-- TODO (E): Display just the first three records to the screen
topThree = LIMIT keyWordSumDesc 3;

--DUMP topThree;
STORE topThree INTO '/dualcore/topFourHighCost1';
