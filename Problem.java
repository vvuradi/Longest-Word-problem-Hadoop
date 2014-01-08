import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Problem {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Set<String> setOfWords = new HashSet<String>();

		@SuppressWarnings("deprecation")
		public void setup(Context context){
			Path[] allPaths = null;
			BufferedReader br = null;
			try {
				allPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());	
				for(Path p : allPaths){
					//reading input from all paths in Distributed Cache
					br = new BufferedReader(new FileReader(p.toString()));
					while(br!=null && br.ready()){
						setOfWords.add(br.readLine());	
					}
					br.close();
				}
			} catch (IOException e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}

		//map function - sends intermediate <Key, Value> as <word, one>
		public void map(LongWritable key, Text value, Context context) throws 
		IOException, InterruptedException {
			
			String line = value.toString();
			setOfWords.remove(line);		
			word.set("WORD");
			if(isConcatenated(setOfWords,line)){				
				context.write(word, new Text(line));				
			}
			setOfWords.add(line);			
		}
		
		/* returns true if the word is concatenated from other 
		   words in the input file */
		public static boolean isConcatenated(Set<String> words, String target) {
			// breaking condition
			if (words.contains(target))
				return true;
			// looks for word with first i characters in set and the 
			// rest of the word is called in recursion.
			for (int i = 1; i < target.length(); i++) {
				if (words.contains(target.substring(0, i))
						&& isConcatenated(words, target.substring(i)))
					return true;
			}
			return false;
		}
	} 
	
	public static class Combine extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			String first = "";
			String second = "";			
			for (Text val : values) {
				sum ++;
				String line = val.toString();
				if(first.length() < line.length()){
	                second = first;
	                first = line;
	            } else if(second.length() < line.length()){
	                second = line;
	            }
			}
			// output from here goes as input to reducer
			context.write(key, new Text(first));
			context.write(key, new Text(second));
			context.write(new Text("count"), new Text(sum+""));			
		}		
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		//reduce function - receives <Key, Value> as <word, one> and 
		//gives output in the form <word, count> 
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			if (key.toString().equals("WORD")) {
				String first = "";
				String second = "";
				for (Text val : values) {
					String line = val.toString();
					if (first.length() < line.length()) {
						second = first;
						first = line;
					} else if (second.length() < line.length()) {
						second = line;
					}
				}
				context.write(new Text("1st Longest word is"), new Text(first));
				context.write(new Text("2nd Longest word is"), new Text(second));
			} else {
				// total words are counted here.
				int sum = 0;
				for (Text val : values) {
					sum += Integer.parseInt(val.toString());
				}
				context.write(new Text("Total count is "), new Text(sum + ""));
			}
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "WordFile");
		job.setJarByClass(Problem.class);		

		//setting key & value classes for output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//setting Mapper and Reducer
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		//setting input and output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//file inputs from user
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//adding file to Distributed Cache
		DistributedCache.addCacheFile(new URI(args[0]), job.getConfiguration());

		job.waitForCompletion(true);
	}

}
