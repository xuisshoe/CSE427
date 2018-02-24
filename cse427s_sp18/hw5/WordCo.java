package stubs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// write a extends method to utilize tool runner 
public class WordCo extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
	  // check if command line arguments are of the right format 
	  // expect [inputpath outputpath]
	  //if length is not 2 print an error message
    if (args.length != 2) {
      System.out.printf("Usage: WordCo <input dir> <output dir>\n");
      return -1;
    }
    /*
     * instantiate job object, specify a jar file,
     * set the job name
     */
    Job job = new Job(getConf());
    job.setJarByClass(WordCo.class);
    job.setJobName("Word Co-Occurrence");
    // set the command line argument to be the input and output path
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	//specify the mapper and reducer classes
	job.setMapperClass(WordCoMapper.class);
	job.setReducerClass(SumReducer.class);
	//specify the output key  and value data type 
	// if mapper output is different, list themn as well
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	// Initiate job and specify job status 
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  /*
   * set up a main method in which we can 
   * actually use the tool runner parameter to run the job
   */

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new WordCo(), args);
    System.exit(exitCode);
  }
}