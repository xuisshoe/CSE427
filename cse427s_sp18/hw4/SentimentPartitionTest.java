package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner spart;

	@Test
	public void testSentimentPartition() {

		spart=new SentimentPartitioner();  // remove type argument to clear error 
		spart.setConf(new Configuration()); //read new configuration and assign to spart
		// now set up three more result in type integer
		int result;
		int result1;
		int result2;
		int result3;
		
		/*
		 * A test for word "beauty" with expected outcome 0  
		 */
		result = spart.getPartition(new Text("beauty"), new IntWritable(23), 3);
		assertEquals(result,0);	
		/*
		 * Test the words "love", expected result is 0 
		 */
		result1 = spart.getPartition(new Text("love"), new IntWritable(23), 3);
		assertEquals(result1,0);	
        
 		/*
		 * Test the word "deadly", expected result is 1
		 */          
		result2 = spart.getPartition(new Text("deadly"), new IntWritable(23), 3);
		assertEquals(result2,1);	
		/*
		 * Test the word "zodiac", expected result is 2
		 */
		result3 = spart.getPartition(new Text("zodiac"), new IntWritable(23), 3);
		assertEquals(result3,2);	
		// end of the test
		
	}

}
