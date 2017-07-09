package edu.uml.cs.yyuan;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * 3.2 Compute Projection By MapReduce
 * Find all the name of the cities and corresponding district
 *-----------------------------------------------------------
 * data:		city.txt.
 * 
 * Map: 		emit <cityName, ditrict>
 * Reduce: 		     <key, values.next()>
 * 
 * @author Yufeng Yuan
 * email: Yufeng_Yuan@student.uml.edu
 * UML_ID: 01506240
 * 
 */
public class ProjectionByMapReduce extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		
		private Text cityName = new Text();
		private Text district = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] list = value.toString().split(","); // Transfer Text value to string and split in to a list
			
			cityName = new Text(list[1]);// City (ID, Name, CountryCode, District, population)
			district = new Text(list[3]);
			
			context.write(cityName, district);

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> ite = values.iterator();   
			context.write(key, new Text(ite.next().toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(ProjectionByMapReduce.class);
		job.setJobName("ProjectionByMapReduce");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("input/city.txt"));
		FileOutputFormat.setOutputPath(job, new Path("result2"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new ProjectionByMapReduce(), args);
		System.exit(ret);
	}

}