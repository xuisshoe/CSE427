package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    /*
     * build an empty string builder and save value into it one by one
     * if there are next input, put a comma in there
     * continue reading till nothing is left
     */
	  
	  StringBuilder stringBuilder = new StringBuilder();

	  for (Text value : values) {
		  stringBuilder.append(value.toString());
		  if (values.iterator().hasNext()) {
			  stringBuilder.append(", ");
		  }
	  }
	  /*
	   *  emit the key and value pair, 
	   *  value being the content saved in the string builder
	   */
	  context.write(key, new Text(stringBuilder.toString()));
  }
}