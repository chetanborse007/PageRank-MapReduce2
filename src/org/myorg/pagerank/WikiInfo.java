/******************************************************************************//*!
* @File          WikiInfo.java
* 
* @Title         MapReduce application for counting the number of wikipedia pages 
* 				 in corpus.
* 				 It consists both types of links,
* 				 	a. Crawled links
* 					b. Links that are not crawled yet
* 
* @Author        Chetan Borse
* 
* @EMail         cborse@uncc.edu
* 
* @Created on    10/31/2016
* 
*//*******************************************************************************/ 


package org.myorg.pagerank;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/******************************************************************************
* @Class		WikiInfo
* @Description	Class representing MapReduce implementation for counting the 
* 				number of wikipedia pages in corpus.
* 				This class implements static 'WikiInfoMap' and 'WikiInfoReducer' 
* 				classes. It also holds job configuration for 'WikiInfo'.
******************************************************************************/
public class WikiInfo extends Configured implements Tool {

	/***************************************************************************
	* @StaticClass	WikiInfoMap
	* @Description	Map class for transforming input splits to intermediate 
	* 				key-value representation such as 
	* 				‘<1>, <Title>’.
	***************************************************************************/
	public static class WikiInfoMap extends Mapper<LongWritable, Text, IntWritable, Text> {

		/* Overridden map method */
		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String title;
			
			String wikiPage = line.toString();
			if (wikiPage == null || wikiPage.trim().isEmpty()) {
				return;
			}
			
			title = wikiPage.split("\\$T\\$A\\$B\\$")[0];
			
			// Emit '<1>, <Title>'
			context.write(new IntWritable(1), new Text(title));
		}
		
	}

	/***************************************************************************
	* @StaticClass	WikiInfoReducer
	* @Description	Reduce class for counting the number of wikipedia pages in 
	* 				corpus.
	*  				The output key-value representation is as below,
	* 				‘<Total wikipedia pages in corpus>, <List of wikipedia pages in corpus>’
	***************************************************************************/
	public static class WikiInfoReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		/* Overridden reduce method */
		@Override 
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
								throws IOException, InterruptedException {
			int count = 0;
			Set<String> titles = new HashSet<String>();
			
			// Iterate through a list of unique wikipedia pages in corpus and 
			// calculate its relevant count.
			for (Text value : values) {
				titles.add(value.toString());
				count++;
			}

			// Emit ‘<Total wikipedia pages in corpus>, <List of wikipedia pages in corpus>’
			context.write(new IntWritable(count), new Text(String.join("$#$", titles)));
		}
		
	}

	/**************************************************************************
	* @Function		getWikiInfo
	* @Description	Function for extracting count and list of wikipedia pages 
	* 				in corpus.
	* @Input		String	wikiInfoPath	Path of wikipedia information
	* 										generated through WikiInfo job.
	* @Return		String[]				Returns string array of count and 
	* 										list of wikipedia pages.
	***************************************************************************/
	public String[] getWikiInfo(String wikiInfoPath) throws IOException {
		Path path;
		FileSystem fs = null;
		BufferedReader br = null;
		String line = null;
		String[] wikiInfo = new String[2];
		
		try {
			path = new Path(wikiInfoPath);
			
            fs = path.getFileSystem(new Configuration());
            
            // Extract a list of files in a given location
            FileStatus[] fileStatus = fs.listStatus(path);
            Path[] files = FileUtil.stat2Paths(fileStatus);
            
            // Iterate through every file and find the exact file where wikipedia
            // information is stored
            for (Path file : files) {
            	if (fs.isFile(file)) {
            		br = new BufferedReader(new InputStreamReader(fs.open(file)));
            		
            		line = br.readLine();
            		
            		// Extract count and list of wikipedia pages from recognized 
            		// file having wikipedia information
            		if (line != null && !line.isEmpty()) {
            			if (line.contains("$T$A$B$")) {
            				wikiInfo[0] = line.split("\\$T\\$A\\$B\\$")[0];
            				wikiInfo[1] = line.split("\\$T\\$A\\$B\\$")[1];
            				break;
            			}
            		}
            	}
            }
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return wikiInfo;
	}

	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 2. It consists 
	* 									input path of link graph over wikipedia
	* 									pages and output path for storing count 
	* 									& list of wikipedia pages.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$T$A$B$");
		conf.set("mapreduce.output.textoutputformat.separator", "$T$A$B$");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName(" Wiki Info ");
		job.setMapperClass(WikiInfoMap.class);
		job.setReducerClass(WikiInfoReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}

