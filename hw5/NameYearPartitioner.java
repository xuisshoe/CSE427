package example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NameYearPartitioner<K2, V2> extends
		HashPartitioner<StringPairWritable, Text> {

	/*
	 * Goal is to globally sort the name and year pair with n reducer
	 * if divide the a to z range into n portion, then each portion should have initial from a to a + increment  
	 * increment = (a to z)/n; should there were any reminders, leave it to the last portion
	 */
	public int getPartition(StringPairWritable key, Text value, int numReduceTasks) {
		//calculate the increment step
		int increment = ('z' - 'a')/numReduceTasks;
		// setup a counter to count each portion
		int n = 0;
		//set up a loop from 1st to last portion to store the value
		for (int i = 'a'; i < 'z' ; i = i + increment){
			// read the last name initial 
			int lastInit = key.getLeft().toLowerCase().charAt(0);
			// if the last name is with any portion go to corresponding reducer 
			if ( (lastInit > (i + increment)) && (lastInit >= i) ){
				return n;
			}
			//update the counter each loop
			n++;			
		}
		return 0;
	}
}
