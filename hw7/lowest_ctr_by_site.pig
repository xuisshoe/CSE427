-- Load only the ad_data1 and ad_data2 directories
-- LOAD '/dualcore/ad_data[12]'
data = LOAD '/dualcore/ad_data1' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray,
             placement:chararray, was_clicked:int, cpc:int);


grouped = GROUP data BY display_site;

by_site = FOREACH grouped {
  -- Include only records where the ad was clicked
	wasClick = FILTER data BY was_clicked == 1; 
  -- count the number of records in this group
	clickCount = COUNT(wasClick);

  /* Calculate the click-through rate by dividing the 
   * clicked ads in this group by the total number of ads
   * in this group.
   */
	N = COUNT(data);
	ctrNum = (double) clickCount/N;
  -- round the ctr to 3 decimal points
	ctrRound = 0.0001 * ((double) ((int)(ctrNum*10000)));
  -- generate the new records
	GENERATE group, ctrRound AS (ctr:double);
}

-- sort the records in ascending order of clickthrough rate
siteDESC = ORDER by_site BY ctr DESC;

-- show just the first four
topFour = LIMIT siteDESC 4;

--DUMP topFour;
STORE topFour INTO '/dualcore/topFourCTR';
