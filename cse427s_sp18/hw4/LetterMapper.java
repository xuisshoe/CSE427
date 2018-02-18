package stubs;
import java.io.IOException;

//*To define a map function for your MapReduce job, subclass 
//the Mapper class and override the map method.
// define the class and data type of mapper input key; input value;
//output key; output value

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import the package for tool runner
import org.apache.hadoop.conf.Configuration;



public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	//set up a parameter for configuration
	private boolean casePara = true; // change to int casePara = 0; if using integer
	/*
	 * The map run once for each line of text in the input file.
	 * It recieve a key of longwritable, a value of text, and output 
	 * key as text and value as intwritable
	 */
	// make a setup method to update the parameter
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		// set casePara to be the configuration parameter, and set true as default (case sensitive default)
		casePara = conf.getBoolean("caseSensitive", true); //switch to this if using integer: conf.getInt("caseSensitive", 1);
	}
    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  /*
	   * Convert the line, which is received as a Text object,
	   * to a String object.
	   */
	  String line = value.toString();
	  //set up a variable name wordInit to store each word. Used for case-transform
	  Text wordInit = new Text(); 
	  
	  // now, divide two case based on different casePara value; 0 for case-sensitive, 1 for case-insensitive.	  
	  if ( casePara == false){      
		  // casePara = 0, case-insensitive case
		  //now, splitting the lines and count the letter of each word
		  for (String word : line.split("\\W+")){
			  if (word.length() > 0) {
				  /*
				   * Call the write method to emit a key and a value from the map method
				   * key: first letter; value: length of the word
				   */				  
				  //set all wordInit to lower case
				  wordInit.set(String.valueOf(word.substring(0,1)).toLowerCase());
				  context.write(new Text(wordInit), new IntWritable(word.length()) );
			  }
			  
		  }
	  }
	  if( casePara == true) {
		  // casePara = 1; case-sensitive case
		  //now, splitting the lines and count the letter of each word
		  for (String word : line.split("\\W+")){
			  if (word.length() > 0) {
				  /*
				   * Call the write method to emit a key and a value from the map method
				   * key: first letter; value: length of the word
				   */				  
				  context.write(new Text(word.substring(0,1)), new IntWritable(word.length()) );
			  }			  
		  }	  
	  }
   
  }
}
