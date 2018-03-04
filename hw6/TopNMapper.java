package stubs;

import java.io.IOException;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * mapper's input is of type long writable (key) and text (value)
 * and return a null-writable and text as output
 *
 */
public class TopNMapper extends
   Mapper<LongWritable, Text, NullWritable, Text> {

   private int N = 3; // default number of ranking
   //build a tree structure to store movie name and corresponding popularity as <vote,name>
   private SortedMap<Double, String> top = new TreeMap<Double, String>();

   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {

      String value2String = value.toString().trim(); // transfer the value to string
      String tokens[] = value2String.split("\\W+"); //split the 
      Double frequency =  Double.parseDouble(tokens[1]);
      String compositeValue = tokens[0] + "," + frequency;
      top.put(frequency, compositeValue);
      // keep only top N of the tree
      if (top.size() > N) {
         top.remove(top.firstKey());
      }
   }
   
   @Override
   /**
    * this method set up the N value for ranking
    */
   protected void setup(Context context) throws IOException,
         InterruptedException {
	// default N is 3, could be change to 10 for question 2
      this.N = context.getConfiguration().getInt("N", 3); 
   }
   
   @Override
   protected void cleanup(Context context) throws IOException,
         InterruptedException {
      for (String str : top.values()) {
         context.write(NullWritable.get(), new Text(str));
      }
   }

}