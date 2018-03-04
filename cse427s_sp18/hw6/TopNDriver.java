package stubs;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
/**
 * This method claimed the usage of the configuration implementation tool
 */
public class TopNDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TopNDriver(), args);
		System.exit(exitCode);
	}
	// with run(args) the configuration parameter can now affect the program 
	public int run(String[] args) throws Exception {
		// check if the argument format is <N> <input> <output>
		if (args.length != 3) {
	         System.out.print("usage TopNDriver <N> <input> <output>");
	         System.exit(1);
	      }
		// set up job name, configuration N, set jar file
		Job job = new Job(getConf());
		job.setJarByClass(TopNDriver.class);
		job.setJobName("Top N");
		int N = Integer.parseInt(args[0]);
		job.getConfiguration().setInt("N", N);
		//set up mapper reducer and num of reducer
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setNumReduceTasks(1);
		// setup mapper reducer input output data type
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//set up input output path
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}
	
}