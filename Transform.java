package org.apache.hadoop.examples;

import java.io.IOException;
import java.io.OutputStream;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Transform{
	public static class TransformMap extends Mapper<LongWritable, Text, Text, Text> {
		//@Override
		public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			//input: <row,col,1>
			//<K,V> = <colId, (rowId, 1)> -> only throw if the value is 1
			String[] data = value.toString().split(",");
			context.write(new Text(data[1]), new Text(data[0]));
			
		}
	}
	
	public static class TransformReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int numOfRows = Integer.parseInt(conf.get("numOfShingles"));//numOfShingles
			int[] row = new int[numOfRows];
			Arrays.fill(row, 0);
			for(Text val : values){
				int rowId = Integer.parseInt(val.toString());
				row[rowId] = 1;
			}
			StringBuilder result = new StringBuilder();
			result.append("M,");
			result.append(key.toString());
			result.append(",");
			for(int i=0; i<numOfRows; i++){
				result.append(Integer.toString(row[i]));
			}
			context.write(null, new Text(result.toString()));
		}
			
	}
	
	public int run(int numOfShingles) throws Exception {
		Configuration conf = new Configuration();
		//Save params
		conf.set("numOfShingles",Integer.toString(numOfShingles));
		
		Job job = new Job(conf,"Transform");
		
		job.setJarByClass(Transform.class);
		job.setMapperClass(TransformMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(TransformReduce.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, "/user/root/data/tempMatrix.txt");
		
		FileOutputFormat.setOutputPath(job, new Path("/user/root/data/transMatrix"));

		return (job.waitForCompletion(true) ? 0 : -1);
	}
}