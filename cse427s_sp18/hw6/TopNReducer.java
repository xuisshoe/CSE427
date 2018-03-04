package stubs;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reducer's input are local top N with their frequency
 * We use a single reducer to creates the final top N.s
 * input of reducer is Nullwritable and text (same as mapperer)
 * output of the reducer is intwritable and text
 */
public class TopNReducer extends
Reducer<NullWritable, Text, IntWritable, Text> implements Configurable{
	
	private int N = 3; // 3 as default
	private Configuration configuration;
	/**
	 * this method sends the configuration 
	 */
	public void setConf(Configuration configuration) {
		
		String thisLine = null;
		File f=new File("/home/cloudera/Downloads/netflix_subset/movie_titles.txt");
		
		try{
			BufferedReader brp = new BufferedReader(new FileReader(f));
			String linep;
			while((linep = brp.readLine()) != null)
			{
				String splitarray[] = linep.split(",");
				title.put(splitarray[0].trim(),splitarray[2].trim());
			}
			brp.close();
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();		
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	public Configuration getConf() {
		return configuration;
	}
	// set up new structure to store title and movie + freq pair
	private SortedMap<Integer, String> top = new TreeMap<Integer, String>();
	private SortedMap<String, String> title = new TreeMap<String, String>();
	
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/*
		 * for all the values coming from the reducer's value
		 * split the line and store movie name and frequency
		 */
		for (Text value : values) {
			String value2String = value.toString().trim();
			String[] tokens = value2String.split(",");
			String id = tokens[0];
			double frequency = Double.parseDouble(tokens[1]);
			int fr = (int) frequency; // cast the freq to int for output 
			top.put(fr, id);
			// keep only top N
			if (top.size() > N) {
				top.remove(top.firstKey());
			}
		}
		// emit final top N from the largest(last in array) to the smallest(first in array)
		List<Integer> keys = new ArrayList<Integer>(top.keySet());
		for(int i=keys.size()-1; i>=0; i--){
			context.write(new IntWritable(keys.get(i)), new Text(title.get(top.get(keys.get(i)))));
		}
	}
	/**
	 * this method set up the N as configuration parameter
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", N); // default is top 3
	}
}