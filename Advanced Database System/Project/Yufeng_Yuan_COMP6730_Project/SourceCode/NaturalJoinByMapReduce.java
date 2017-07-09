package edu.uml.cs.yyuan;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * 3.3 Computing Natural Join by MapReduce
 * Find all countries whose official language is English.
 *-----------------------------------------------------------
 * data: country.txt   countryLanguage.txt
 * 
 * MapCountryLanguage:     emit <countryCode, "FindCountryCode">
 * MapConutry:             emit <countryCode, countryName>              
 * Reduce:  	           for any key if it has two values, that means it's official language is Englisht
 * 						        <countryName, "">
 * 
 * @author Yufeng Yuan
 * email: Yufeng_Yuan@student.uml.edu
 * UML_ID: 01506240
 * 
 */
public class NaturalJoinByMapReduce extends Configured implements Tool {

	public static class MapCountryLanguage extends Mapper<Object, Text, Text, Text> {
		
		private Text countryCode = new Text();
		private Text FindCountryCode = new Text("FindCountryCode");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] list = value.toString().split(","); // Transfer Text value to string and split in to a list
			
			countryCode = new Text(list[0]);//CountryLanguage (CountryCode, Language, IsOfficial, Percentage)
			String language = list[1];
			String isOfficial = list[2];
			/*
			 * Check if "English" is the country's official language 
			 */
			if (language.equals("English") && isOfficial.equals("T")){
				// System.out.println("testing" + countryCode);
				context.write(countryCode, FindCountryCode);
			}
		}
	}
			
	public static class MapCountry extends Mapper<Object, Text, Text, Text> {
		
		private Text countryCode = new Text();
		private Text countryName = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] list = value.toString().split(",");// Transfer Text value to string and split in to a list

			
			countryCode = new Text(list[0]);//Country (Code, Name, Continent, Region, ..., Code2)
			countryName = new Text(list[1]);
			
			context.write(countryCode, countryName);
			
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private Text countryEn = new Text();
		private Text emptyText = new Text("");
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int counter = 0;
			Iterator<Text> ite = values.iterator();
			while (ite.hasNext()) {
				String list = ite.next().toString();
				if (!list.equals("FindCountryCode")) {
					countryEn = new Text(list);
				}
				counter++;
			}
			if (counter == 2){
				context.write(countryEn, emptyText);
			}
		}
			
	}


	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(NaturalJoinByMapReduce.class);
		job.setJobName("NaturalJoinByMapReduce");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapCountry.class);
		job.setMapperClass(MapCountryLanguage.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Here we use MultipleInputs instead of FileInputFormat because we need two map class
		MultipleInputs.addInputPath(job, new Path("input/country.txt"), TextInputFormat.class, MapCountry.class);
		MultipleInputs.addInputPath(job, new Path("input/countrylanguage.txt"), TextInputFormat.class, MapCountryLanguage.class);
		
		FileOutputFormat.setOutputPath(job, new Path("result3"));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new NaturalJoinByMapReduce(), args);
		System.exit(ret);
	}

}
