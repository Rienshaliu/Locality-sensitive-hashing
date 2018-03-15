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

public class LocalitySensitiveHash{
	public static class LSHMap extends Mapper<LongWritable, Text, Text, Text> {
		//@Override
		public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			String rowPerBand = conf.get("rowPerBand");
			String[] data = value.toString().split(",");
			int bandNum = Integer.parseInt(data[0])/Integer.parseInt(rowPerBand);
			int newRowId = Integer.parseInt(data[0])%Integer.parseInt(rowPerBand);
			context.write(new Text(Integer.toString(bandNum)), new Text(Integer.toString(newRowId)+","+data[1]+","+data[2]));
			
		}
	}
	
	public static class LSHReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			int rowPerBand = Integer.parseInt(conf.get("rowPerBand"));
			int document = Integer.parseInt(conf.get("document"));
			
			int[][] multi = new int[document][rowPerBand];
			HashMap<String, List<Integer>> LSH = new HashMap<String, List<Integer>>();


			for(Text val : values){
				String[] data = val.toString().split(",");
				int rowId = Integer.parseInt(data[0]);
				int colId = Integer.parseInt(data[1]);
				int value = Integer.parseInt(data[2]);
				multi[colId][rowId] = value; //inserse row & column
			}
			for(int i=0;i<document;i++){
				StringBuilder signature = new StringBuilder();
				for(int j=0;j<rowPerBand;j++){
					signature.append(multi[i][j]);
				}
				if(LSH.get(signature.toString()) == null){
					LSH.put(signature.toString(),new ArrayList<Integer>());
				}
				List<Integer> temp = LSH.get(signature.toString());
				temp.add(i);
				LSH.put(signature.toString(),temp);
			}
			

			for (Object keyItr : LSH.keySet()){
				if(LSH.get(keyItr).size() >= 2){
					context.write(null, new Text(LSH.get(keyItr).toString()));
				}
			}
			/*
			for (Text val : values) {
				context.write(new Text(key.toString()),new Text(val.toString()));
			}
			*/
		}
			
	}
	
	public int run(int rowPerBand,int document) throws Exception {
		Configuration conf = new Configuration();
		//Save params
		conf.set("rowPerBand",Integer.toString(rowPerBand));
		conf.set("document",Integer.toString(document));
		
		Job job = new Job(conf,"LSH");
		
		job.setJarByClass(LocalitySensitiveHash.class);
		job.setMapperClass(LSHMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(LSHReduce.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, "/user/root/data/Signature.txt");
		
		FileOutputFormat.setOutputPath(job, new Path("/user/root/data/result"));

		return (job.waitForCompletion(true) ? 0 : -1);
	}
}