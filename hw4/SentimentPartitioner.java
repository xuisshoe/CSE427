package stubs;

import java.util.HashSet;
// import package for read and save the file 
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

// set sentiment partitioner, with text key(actual word) and intwritable value(positive or negative word), 
public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {
  // claim parameter
  private Configuration configuration;
  //claim hash set to store positive and negative words as 
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();


  @Override
  public void setConf(Configuration configuration) {
    /*
     * setup variables within setConf 
     * add positive and negative words to corresponding sets
     */
	 this.configuration = configuration; // set up variable 
	 //read two words files
	 File poWordsFile = new File("/home/cloudera/workspace/sentimentpartition/positive-words.txt");
	 File neWordsFile = new File("/home/cloudera/workspace/sentimentpartition/negative-words.txt");
	 
	 // try to fit the word to positive file
	 try {
		 FileReader fileReadPo = new FileReader(poWordsFile);
		 Scanner scp = new Scanner(fileReadPo);
		// use while loop to read through the file
		 while(scp.hasNext()){
			// check if the word is within positive word file
			 String pWord = scp.next();
			 // if the word is not start by ";", then count as a word
			 if(!(pWord.charAt(0) == ';')){
				 positive.add(pWord);
			 }
		 }
		 // close file after one round of searching
		 scp.close();
		 fileReadPo.close();
	 }	
	 catch (IOException e){
		 System.out.println(e);	 
	 }
	 // try to fit the word to negative word
	 try {
		 FileReader fileReadNe = new FileReader(neWordsFile);
		 Scanner scp = new Scanner(fileReadNe);
		 // use while loop to read through the file
		 while(scp.hasNext()){
			 // check if the word is within negative word file
			 String nWord = scp.next();
			 // if the word is not start by ";", then count as a word
			 if(!(nWord.charAt(0) == ';')){  
				 negative.add(nWord);
			 }
		 }
		 // close file after one round of searching
		 scp.close();
		 fileReadNe.close();
	 }	
	 catch (IOException e){
		 System.out.println(e);	 
	 }
	 
	 
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You need to implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    
	 // set up if statements to clarify each output using positive.contains(key.toString()) 
	 //and negative.contains(key.toString())
	  if (positive.contains(key.toString())){
		  // if it is in the positive then return 0, 
		  return 0;
	  }
	  else if (negative.contains(key.toString())){
		  // if it is in the negative then return 1,
		  return 1;
	  }
	  else {
		  // if not in positive nor negative then return 2
		  return 2;
	  }
  }
}
