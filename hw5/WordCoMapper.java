package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  	/*
	  	 * save the text input as string 
	  	 * and split each word 
	  	 */
	    String line = value.toString(); 
        String lineSplit[] = line.split("\\W+");
        /*
         * for each word in the line, loop throw each token and read the current and next word
         * save the current and next word as key 
         * emit a number 1 as value, thus this value of 1 could then also be a counter 
         * to count the word pair 
         */
	    for (int i =0;i<lineSplit.length-1;i++) {
	        String word1 = lineSplit[i].toLowerCase();
	        String word2 = lineSplit[i+1].toLowerCase();
	      if (word1.length() > 0 && word2.length()>0) {	        
	        context.write(new Text(word1.toLowerCase()+","+word2.toLowerCase()), new IntWritable(1));
	      }
	    }
    
  }
}