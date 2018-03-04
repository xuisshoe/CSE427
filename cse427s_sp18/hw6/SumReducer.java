package stubs;

import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This method is the main method of the reducer 
 * the input data type is text, doublewritable which matches with the mapper 
 * the output of the reducer is text and double writable
 */   
public class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  
  @Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0; // set up sum counter 
		
		/*
		 * For each value in values passed to the reducer
		 * add the sum wo the word count for corresponding key
		 */
		for (DoubleWritable value : values) {
			sum += value.get();
		}
		
		// emit a key value pair, key has not changed, the new value is the sum of the same key
		context.write(key, new DoubleWritable(sum));
	}
}