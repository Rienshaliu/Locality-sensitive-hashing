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

public class Shingling{
	
	public static FileSystem fs;
	
	public static class ShinglingMap extends Mapper<Text, Text, Text, Text> {
		//@Override
		public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
			//<K,V> = <"First Capital", ()>
			Configuration conf = context.getConfiguration();
			int sizeOfShingle = Integer.parseInt(conf.get("sizeOfShingle"));
			
			String data = value.toString();
			
			for(int i=0;i<data.length()-sizeOfShingle+1;i++){
				StringBuilder shingle = new StringBuilder();
				//context.write(new Text(String.valueOf(data.charAt(i))),new Text(""));
				
				for(int j=i;j<=(i+sizeOfShingle-1);j++){
					shingle.append(String.valueOf(data.charAt(j)));
				}
				
				String keyName = shingle.toString();
				context.write(new Text(String.valueOf(keyName.charAt(0))),new Text(keyName+key.toString()));
			}
			//context.write(new Text("JOJO"),new Text(value.toString()));
			
		}
	}
	
	public static class ShinglingReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			int sizeOfShingle = Integer.parseInt(conf.get("sizeOfShingle"));
			int numOfDocument = Integer.parseInt(conf.get("numOfDocument"));
			
			HashMap<String, List<Integer>> M = new HashMap<String, List<Integer>>();
			for (Text val : values) {
				String str = val.toString();
				StringBuilder Shingle = new StringBuilder();
				for(int i=0;i<sizeOfShingle;i++) Shingle.append(String.valueOf(str.charAt(i)));
				StringBuilder Document = new StringBuilder();
				for(int i=sizeOfShingle;i<str.length();i++) Document.append(String.valueOf(str.charAt(i)));
				
				if(M.get(Shingle.toString()) == null){
					M.put(Shingle.toString(),new ArrayList<Integer>());
				}
				List<Integer> temp = M.get(Shingle.toString());
				temp.add(Integer.parseInt(Document.toString()));
				M.put(Shingle.toString(),temp);
			}
			//for every shingle
			for (Object keyItr : M.keySet()){
				List<Integer> temp = M.get(keyItr);
				int[] doc = new int[numOfDocument];
				//initilize doc
				for(int i=0; i<numOfDocument; i++){
					doc[i] = 0;
				}
				//traverse the temp array
				for(int i=0; i<temp.size(); i++){
					if(doc[temp.get(i)-1] == 0){
						doc[temp.get(i)-1] = 1;
					}
				}
				String mat = "M,";
				for(int i=0; i<doc.length;i++){
					mat += Integer.toString(doc[i]);
					if(i!=doc.length-1){
						mat += ",";
					}
				}
				//<row, col, val> = <"A1", ,>
				context.write(null, new Text(mat));
			}
			/*
			for (Text val : values) {
				context.write(new Text(key.toString()),new Text(key.toString()+","+val.toString()));
			}*/
		}
			
	}
	
	public int run(int numOfDocument, int sizeOfShingle) throws Exception {
		Configuration conf = new Configuration();
		//Save params
		conf.set("mapred.textoutputformat.separator", ",");
		conf.set("key.value.separator.in.input.line", ",");
	
		conf.set("numOfDocument",Integer.toString(numOfDocument));
		conf.set("sizeOfShingle",Integer.toString(sizeOfShingle));
		
		Job job = new Job(conf,"Shingling");
		
		job.setJarByClass(Shingling.class);
		job.setMapperClass(ShinglingMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(ShinglingReduce.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, "/user/root/data/data.txt");
		
		FileOutputFormat.setOutputPath(job, new Path("/user/root/data/ShinglingMatrix"));

		return (job.waitForCompletion(true) ? 0 : -1);
	}
	
}