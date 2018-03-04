package stubs;


import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;


/* 
 * The following is the code for the aggregate rating driver class
 * the first method claim the usage of configuration implementation tool
 */
public class AggregateRatings extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new AggregateRatings(),args);
		System.exit(exitCode); 
		 }
    // with run(args) the configuration parameter can now affect the program
	public int run(String[] args) throws Exception  { 
		// check the argument format:<input dir> <output dir>
    if (args.length != 2) {
      System.out.printf(
          "Usage: AggregateByKeyDriver <input dir> <output dir>\n");
     return -1;
    }
    // setup mew job name and jar class
    Job job = new Job(getConf()); 
    job.setJarByClass(AggregateRatings.class);
    job.setJobName("AggregateByKeyDriver");
    /*
     * Specify the paths to the input and output data based on the
     * command-line arguments.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(AggregateRatingsMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setCombinerClass(SumReducer.class);   
    //set output data type of the reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    // check job status
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;

  }


}
