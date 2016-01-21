package com.example.MyWordCount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.example.MyWordCount.MyMapper;
import com.example.MyWordCount.MyReducer;


public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	     
		 // Create a new Job
	     Job job = Job.getInstance();
	     job.setJarByClass(WordCount.class);
	     
	     // Specify various job-specific parameters     
	     job.setJobName("myWordCount");
	     
	     job.setInputFormatClass(TextInputFormat.class);
	     job.setOutputFormatClass(TextOutputFormat.class);
	     TextInputFormat.addInputPath(job, new Path(args[0]));
	     TextOutputFormat.setOutputPath(job, new Path(args[1]));
	     
	     job.setMapperClass(MyMapper.class);
	     job.setReducerClass(MyReducer.class);

	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
	     // Submit the job, then poll for progress until the job is complete
	     job.waitForCompletion(true);

	}

}
