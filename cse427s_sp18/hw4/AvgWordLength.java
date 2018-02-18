package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* 
 * We need to add extend configure tool to utilize tool runner
 */
public class AvgWordLength extends Configured implements Tool{
	/*
	 *The following public void setup is to make sure that the setup method will
	 *be executed before the map or reduce method is called
	 */
   public static void main(String[] args) throws Exception {
	  // setup the exit code as a parameter for tool runner
	  int exitCode = ToolRunner.run(new Configuration(), new AvgWordLength(),args);
	  System.exit(exitCode);
   	}
    /*
     * The expected command-line arguments are the paths containing
     * input and output data. Terminate the job if the number of
     * command-line arguments is not exactly 2.
     */
   public int run(String[] args) throws Exception {	
	   // the run() function is critical here because it utilizes the argument
	   if (args.length != 2) {
		      System.out.printf(
		          "Usage: WordLength <input dir> <output dir>\n");
		      System.exit(-1);  
       }
    /*
     * Instantiate a Job object for your job's configuration.  
     * it is important to use "getConf" as this command retrieve the parameter set
     * in the mapper 
     */
    Job job = new Job(getConf());
    /*
     * Specify the jar file that contains driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running mapper and reducer tasks.
     */
    job.setJarByClass(AvgWordLength.class);   
    /*
     * Specify an name for the job. This job name will appear in reports and logs.
     */
    job.setJobName("Avg Word Length");
    /*
     * Specify the paths to the input and output data based on the command-line arguments.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(LetterMapper.class);
    job.setReducerClass(AverageReducer.class);
    /*
     * Specify the job's output key and value classes. 
     * If mapper output and reducer input are of different class, set the 'map-output' separately.
     */
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    /*
     * Start the MapReduce job, if it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    /*
     * Need to utilize a return function to return the map-reduce job result
     * As return is the expected java grammar here
     */
    return success ? 0:1;
   }

}


