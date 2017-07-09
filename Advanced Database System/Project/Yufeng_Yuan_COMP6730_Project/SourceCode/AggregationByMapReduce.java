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
 * 3.4 Aggregation by MapReduce
 * Find how many cities each district has.
 *-----------------------------------------------------------
 * data: 		city.txt
 * 
 * This question is similiar to the offical sample WordCount.
 * The different with question 1 is that this time in map need use ditrict as our key
 * 
 * Map:			<District, one>
 * Reduce:		<District, sum(one)>
 * 
 * @author Yufeng Yuan
 * email: Yufeng_Yuan@student.uml.edu
 * UML_ID: 01506240
 * 
 */
public class AggregationByMapReduce extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		
		private IntWritable one = new IntWritable(1);
		private Text district = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] list = value.toString().split(",");
			
			district = new Text(list[3]);  //City (ID, Name, CountryCode, District, population)
			
			context.write(district, one);
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
		      for (IntWritable val : values) {
		        sum += val.get();
		      }
		      result.set(sum);
		      context.write(key, result);
		    }
		}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(AggregationByMapReduce.class);
		job.setJobName("AggregationByMapReduce");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("input/city.txt"));
		FileOutputFormat.setOutputPath(job, new Path("result4"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new AggregationByMapReduce(), args);
		System.exit(ret);
	}

}