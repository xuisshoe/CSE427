package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.StringTokenizer;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {
	// boolean parameter to control case sensitive 
	private boolean caseSensitive = true;
	// set up text variable word and filename for storing the corresponding info
    private Text word =new Text();
    private Text filename= new Text();
	
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {

    /*
     * retrieve the file name and save as string
     * save the input fron text to string
     */
	  String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
	  filename = new Text(filenameStr);
	  String line = value.toString();
	  // set up a case selector that reads the case sensitive parameter 
	  if (!caseSensitive) {
		  line = line.toLowerCase();
	  }
	  /*
	   * tokenize the line 
	   * while there is still more text, continue tokenize
	   * emit the word and its corresponding file and line position 
	   * as key value pair
	   */
	  StringTokenizer tokenizer = new StringTokenizer(line);
	  while (tokenizer.hasMoreTokens()) {
		  word.set(tokenizer.nextToken());			
		  context.write(word,new Text(filename+"@"+key) );
	  }
  }
}