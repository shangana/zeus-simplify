package com.leju.wordcount;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			System.out.println(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
				// System.out.println("itr.hasMoreTokens():
				// "+itr.hasMoreTokens()+" itr.nextToken(): "+itr.nextToken());
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		String[] otherArgs = new String[2];
		otherArgs[0] = "hdfs://10.207.0.217:9000/data/test_in/Test.txt";
		String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		otherArgs[1] = "hdfs://10.207.0.217:9000/data/test_out/mr-" + time;
        //默认读取classpath上的core-site.xml构造Configuration
		Job job = Job.getInstance();

		job.setJobName("word count " + time);
		job.setJarByClass(WordCount.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//设置mapred-site.xml和yarn-site.xml的重要配置
		Configuration conf = job.getConfiguration();
		conf.set("fs.default.name", "hdfs://10.207.0.217:9000");
		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.jobtracker.address", "10.207.0.230:9001");
		conf.set("yarn.resourcemanager.hostname", "10.207.0.230");
		conf.set("yarn.resourcemanager.admin.address", "10.207.0.230:8033");
		conf.set("yarn.resourcemanager.address", "10.207.0.230:8032");
		conf.set("yarn.resourcemanager.resource-tracker.address", "10.207.0.230:8031");
		conf.set("yarn.resourcemanager.scheduler.address", "10.207.0.230:8030");
		conf.set("mapreduce.jobhistory.address" , "10.207.0.230:10020" );
		System.out.println("Job start!" + job.getConfiguration().get("mapreduce.job.jar"));
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(true)) {
			System.out.println("ok!");
			long startTime = job.getStartTime();
			long finishTime = job.getFinishTime();
			System.out.println(finishTime - startTime);
		} else {
			System.out.println("error!");
			System.exit(0);
		}
	}
}
