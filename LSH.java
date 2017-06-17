package org.apache.hadoop.examples;

import java.io.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;

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
/**
 * Hello world!
 *
 */
public class LSH
{
    public static Configuration conf = new Configuration();
	public static FileSystem fs;
	public static int document = 10;
	public static int row = 5;
	public static int sizeOfShingle = 9;
	public static int sizeOfSignature = 500;
	public static int numOfShingles;
	
	public static void main(String[] args) throws Exception {
    
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: LSH <in> <out>");
			System.exit(2);
		}
		
		String input = "/user/root/data/";
		dealingText(input);
		
		Path outputPath = new Path("/user/root/data/ShinglingMatrix");
	    outputPath.getFileSystem(conf).delete(outputPath, true);
		//if (fs.exists(new Path("/user/root/data/ShinglingMatrix"))) {fs.delete(new Path("/user/root/data/ShinglingMatrix"), true);}
		Shingling S = new Shingling();
		int complete = S.run(document, sizeOfShingle);
		
		fs = FileSystem.get(conf);
		Path mergePath = new Path("/user/root/data/Matrix.txt");
		mergePath.getFileSystem(conf).delete(mergePath, true);
		FileUtil.copyMerge(fs, outputPath, fs, mergePath, true, conf, null);
		
		fs = mergePath.getFileSystem(conf);
		//Buffer Reader/Buffer Writer
		Path tempMatrixPath = new Path("/user/root/data/tempMatrix.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(mergePath)));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(tempMatrixPath, true), "UTF-8"));
		String line = br.readLine();
		int rowId = 0;
		while(line != null){
			String[] vals = line.split(",");
			for(int i=1; i<vals.length; i++){
				//row, col, value
				//we don't need to store 0
				if(Integer.parseInt(vals[i]) == 1){
					bw.write(Integer.toString(rowId) + "," + Integer.toString(i-1) + "," + vals[i] + "\n");
				}
			}
			line = br.readLine();
			rowId++;
		}
		bw.flush();
		bw.close();
		//do permutation
		numOfShingles = permutation();
		//do transformation
		Path transformerPath = new Path("/user/root/data/transMatrix");
	    transformerPath.getFileSystem(conf).delete(transformerPath, true);
		Transform tf = new Transform();
		tf.run(numOfShingles);
		
		fs = FileSystem.get(conf);
		Path transPath = new Path("/user/root/data/transMatrix.txt");
		transPath.getFileSystem(conf).delete(transPath, true);
		FileUtil.copyMerge(fs, transformerPath, fs, transPath, true, conf, null);
		//do minhashing
		//delete file
		Path sigPath = new Path("/user/root/data/Signature");
		sigPath.getFileSystem(conf).delete(sigPath, true);
		//run
		MinHashing minhashing = new MinHashing();
		minhashing.run(numOfShingles, sizeOfSignature, document);
		
		fs = FileSystem.get(conf);
		Path signaturePath = new Path("/user/root/data/Signature.txt");
		signaturePath.getFileSystem(conf).delete(signaturePath, true);
		FileUtil.copyMerge(fs, sigPath, fs, signaturePath, true, conf, null);
		
		Path resultPath = new Path("/user/root/data/result");
		resultPath.getFileSystem(conf).delete(resultPath, true);
		LocalitySensitiveHash lshing = new LocalitySensitiveHash();
		lshing.run(row,document);
    }
	
    public static void dealingText(String input)throws IOException{
		fs = FileSystem.get(conf);
		Path initailPath = new Path("/user/root/data/data.txt");
		if (fs.exists(initailPath)) {fs.delete(initailPath, true);}
		BufferedWriter brW=new BufferedWriter(new OutputStreamWriter(fs.create(initailPath,true)));
		
		for (int i=1;i<=document;i++){
			Path inputPath = new Path(input+"news"+Integer.toString(i)+".txt");
			
			brW.write(i+",");
			
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			String line;
			String[] temp;
			
			StringBuilder result = new StringBuilder();
			line=br.readLine();
			while (line != null){
				temp = line.split(" +");
				for (String word:temp){
					result.append(word);
					result.append(" ");
				}
				line=br.readLine();
			}
			
			brW.write(result.toString());
			brW.write("\n");
		}
		brW.close();
    }
	public static int permutation()throws IOException{
		
		Path matrixPath = new Path("/user/root/data/Matrix.txt");
		FileSystem fs = matrixPath.getFileSystem(conf);
		List<String> line_temp = new ArrayList<String>();
		line_temp = IOUtils.readLines(fs.open(matrixPath), "UTF8");
		System.out.println(line_temp.size());
		int num_shingle = line_temp.size();
		
		List<Integer> solution = new ArrayList<>();
		for (int i=0;i<num_shingle;i++) {
			solution.add(i);
		}
		
		Path permutationPath = new Path("/user/root/data/Permutation.txt");
		OutputStream os = fs.create(permutationPath);
		for (int i=0;i<sizeOfSignature;i++){
			Collections.shuffle(solution);
			IOUtils.write("P,"+Integer.toString(i)+",", os);
			for (int j=0;j<solution.size();j++){
				IOUtils.write(Integer.toString(solution.get(j)), os);
				if (j!=solution.size()-1){
					IOUtils.write("-", os);
				}
			}
			IOUtils.write("\n", os);
		}
		os.close();
		return num_shingle;
	}
}
