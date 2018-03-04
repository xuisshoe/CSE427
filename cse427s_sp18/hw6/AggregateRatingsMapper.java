package stubs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * this is the main method of the aggregate mapper 
 * mapper's input is of type long writable (key) and text (value)
 * and return a null-writable and text as output
 */
public class AggregateRatingsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line Text object to a String object.
     */
    String line = value.toString().trim();
    String[] tokens =line.split(","); // split the line 
    
    if (tokens.length != 3) {
        return;
     }
    // emit the movie name(id) and the corresponding rating 
    String id = tokens[0]; 
    double rating = Double.parseDouble(tokens[2]); 
    context.write(new Text(id), new DoubleWritable(rating));
  }
}