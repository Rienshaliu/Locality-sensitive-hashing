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

public class MinHashing{
	
	public static FileSystem fs;
	
	
	public static class MinHashingMap extends Mapper<LongWritable, Text, Text, Text> {
		//@Override
		public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			//<K,V> = <(permutationID,documentID), ()>
			String[] data = value.toString().split(",");
			String matrix = data[0];
			//get conf
			Configuration conf = context.getConfiguration();
			int numOfSignatures = Integer.parseInt(conf.get("numOfSignatures"));
			int numOfDocuments = Integer.parseInt(conf.get("numOfDocuments"));
			//get the total amount of documents
			if (matrix.contains("M")){
				for(int i=0; i<numOfSignatures; i++){
					context.write(new Text(Integer.toString(i)+","+data[1]), new Text("M," + data[2]));
				}
			}
			if (matrix.contains("P")){
				for(int i=0; i<numOfDocuments;i++){
					context.write(new Text(data[1]+","+Integer.toString(i)), new Text("P," + data[2]));
				}
			}
		}
	}
	
	public static class MinHashingReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//get conf
			Configuration conf = context.getConfiguration();
			int numOfShingles = Integer.parseInt(conf.get("numOfShingles"));
			int[] per = new int[numOfShingles];
			boolean[] mat = new boolean[numOfShingles];
			for(int i=0; i<numOfShingles; i++){
				mat[i] = false;
			}
			for(Text value:values){
				String[] data = value.toString().split(",");
				if(data[0].equals("M")){
					String content = data[1];
					for(int i=0; i<content.length(); i++){
						if(String.valueOf(content.charAt(i)).equals("0")){
							mat[i] = false;
						}else{
							mat[i] = true;
						}
					}
				}else{
					String[] rawPer = data[1].split("-");
					for(int i=0; i<rawPer.length; i++){
						per[i] = Integer.parseInt(rawPer[i]);
					}
				}
			}
			int k=1;
			for(int i=0; i<per.length; i++){
				if(mat[per[i]]){
					//record k
					context.write(null, new Text(key.toString() + "," + Integer.toString(k)));
					break;
				}else{
					k++;
				}
			}
		}
			
	}
	
	public int run(int numOfShingles, int numOfSignatures, int document) throws Exception {
		//numOfdocuments
		//numOfsignatures
		Configuration conf = new Configuration();
		//Save params
		conf.set("numOfShingles",Integer.toString(numOfShingles));
		conf.set("numOfSignatures", Integer.toString(numOfSignatures));
		conf.set("numOfDocuments", Integer.toString(document));
		Job job = new Job(conf,"MinHashing");
		
		job.setJarByClass(MinHashing.class);
		job.setMapperClass(MinHashingMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(MinHashingReduce.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, "/user/root/data/transMatrix.txt,/user/root/data/Permutation.txt");
		
		FileOutputFormat.setOutputPath(job, new Path("/user/root/data/Signature"));

		return (job.waitForCompletion(true) ? 0 : -1);
	}
	
}