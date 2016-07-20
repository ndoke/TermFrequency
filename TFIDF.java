/*
*NACHIKET DOKE
* 800878686
* ndoke@uncc.edu
*/

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.Scanner;
import java.util.HashMap;
import java.io.File;
import java.text.DecimalFormat;
import java.lang.Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);

	public static double numDocs = 0;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job1 = Job.getInstance(getConf(), " TFIDF1 ");
		job1.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job1, args[0]);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] files = fs.listStatus(new Path(args[0]));
		numDocs = files.length;

		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		Configuration configuration = new Configuration();
		configuration.set("numberOfFiles", numDocs + "");
		Job job2 = Job.getInstance(configuration, " TFIDF2 ");
		job2.setJarByClass(this.getClass());

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job1.waitForCompletion(true);
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String fName = new String();
			fName = ((FileSplit) context.getInputSplit()).getPath().getName();

			String line = lineText.toString();
			Text currentWord = new Text();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				currentWord = new Text(word + "#####" + fName);
				context.write(currentWord, one);
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			double termFreq;

			for (IntWritable count : counts) {
				sum += count.get();
			}

			termFreq = 1 + Math.log10(sum);

			context.write(word, new DoubleWritable(termFreq));
		}
	}

//New Map and reduce classes for next pass
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {

		private Text wordAndDoc = new Text();
		private Text wordAndCounters = new Text();

		public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {

			String[] wordAndCounters = value.toString().split("\t");
			String[] wordAndDoc = wordAndCounters[0].split("#####");
			this.wordAndDoc.set(new Text(wordAndDoc[0]));
			this.wordAndCounters.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
			context.write(this.wordAndDoc, this.wordAndCounters);
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

		private Text wordAtDocument = new Text();
		private Text tfidfCounts = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int numKeyFiles = 0;
			
			//Accessing the number of files from thee outer class
			Configuration conf = context.getConfiguration();
			String temp = conf.get("numberOfFiles");
			Double numberOfFiles = Double.parseDouble(temp);
			
			HashMap<String, String> tempFrequencies = new HashMap<String, String>();

			for (Text val : values) {
				String[] docFreq = val.toString().split("=");

				if (Double.parseDouble(docFreq[1].split("/")[0]) > 0) {
					numKeyFiles += Double.parseDouble(docFreq[1]);
				}
				tempFrequencies.put(docFreq[0], docFreq[1]);

			}

			for (String document : tempFrequencies.keySet()) {
				String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");

				//calculating TF, IDF & TF-IDF
				double tf = Double.parseDouble(tempFrequencies.get(document));

				double idf = Math.log10(numberOfFiles / (double) ((numKeyFiles == 0 ? 1 : 0)
						+ numKeyFiles));

				double tfIdf = tf * idf;

				this.wordAtDocument.set(key + "#####" + document);
				this.tfidfCounts.set(Double.toString(tfIdf));

				context.write(this.wordAtDocument, this.tfidfCounts);
			}
		}
	}
}
