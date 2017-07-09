package edu.uml.cs.yyuan;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * 3.1 Computing Selection By MapReduce:
 * Find cities whose population is larger than 300,000
 *-----------------------------------------------------------
 * data:		city.txt.
 * 
 * Map:     	check if  population > 300,000, then emit <cityName, "">
 * Reduce:  	we just simply output those cities, <key, "">
 * 
 * @author Yufeng Yuan
 * email: Yufeng_Yuan@student.uml.edu
 * UML_ID: 01506240
 * 
 */
public class SelectionByMapReduce extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		
		private Text cityName = new Text();
		private Text emptyText = new Text("");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] list = value.toString().split(","); // Transfer Text value to string and split in to a list
			
			cityName = new Text(list[1]);// City (ID, Name, CountryCode, District, population)
			
			long population = Integer.parseInt(list[4]);
			if (population > 300000){
				context.write(cityName, emptyText);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private Text emptyText = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, emptyText);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(SelectionByMapReduce.class);
		job.setJobName("SelectionByMapReduce");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("input/city.txt"));
		FileOutputFormat.setOutputPath(job, new Path("result1"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SelectionByMapReduce(), args);
		System.exit(ret);
	}

}