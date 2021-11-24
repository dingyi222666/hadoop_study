package github.dingyi222666.hadoop.service;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopService {

	public static String HADOOP_MASTER_PATH = "master";
	public static String HADOOP_HDFS_PORT = "9000";

	private String hadoopPath = "";
	private String hadoopPort = "";

	private Configuration configuration;

	public HadoopService() {
		this(HADOOP_MASTER_PATH, HADOOP_HDFS_PORT);
	}

	public HadoopService(String hadoopPath, String hadoopPort) {
		this.hadoopPath = hadoopPath;
		this.hadoopPort = hadoopPort;
		configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://" + hadoopPath + ":" + hadoopPort);

	}

	public static Configuration createConfiguration() {
		return new HadoopService().getConfiguration();
	}

	public Path wapperPath(String path) {
		return new Path("hdfs://" + hadoopPath + ":" + hadoopPort + path);
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public static class HadoopTask {

		private Class<? extends Mapper> mapperClass;

		private Class<? extends Reducer> reducerClass;

		private Class<?> runClass;

		private Class<? extends Writable> mapOutputKeyClass;
		private Class<? extends Writable> mapOutputValueClass;

		private String taskName;

		public String getTaskName() {
			return taskName;
		}

		public void setTaskName(String taskName) {
			this.taskName = taskName;
		}

		public Class<? extends Reducer> getCombinerClass() {
			return combinerClass;
		}

		public void setCombinerClass(Class<? extends Reducer> combinerClass) {
			this.combinerClass = combinerClass;
		}

		private Class<? extends Writable> outputKeyClass;

		private Class<? extends Writable> outputValueClass;

		private List<Path> inputPaths = new ArrayList<>();

		private Path outputPath = new Path("/output");

		private Class<? extends Reducer> combinerClass;

		private Class<? extends RawComparator> groupingComparatorClass;

		public Class<? extends RawComparator> getGroupingComparatorClass() {
			return groupingComparatorClass;
		}

		public void setGroupingComparatorClass(Class<? extends RawComparator> groupingComparatorClass) {
			this.groupingComparatorClass = groupingComparatorClass;
		}

		public Class<? extends Writable> getMapOutputValueClass() {
			return mapOutputValueClass;
		}

		public void setMapOutputValueClass(Class<? extends Writable> mapOutputValueClass) {
			this.mapOutputValueClass = mapOutputValueClass;
		}

		public Class<? extends Mapper> getMapperClass() {
			return mapperClass;
		}

		public void setMapperClass(Class<? extends Mapper> mapperClass) {
			this.mapperClass = mapperClass;
		}

		public Class<? extends Reducer> getReducerClass() {
			return reducerClass;
		}

		public void setReducerClass(Class<? extends Reducer> reducerClass) {
			this.reducerClass = reducerClass;
		}

		public Class<?> getRunClass() {
			return runClass;
		}

		public void setRunClass(Class<?> runClass) {
			this.runClass = runClass;
		}

		public Class<? extends Writable> getMapOutputKeyClass() {
			return mapOutputKeyClass;
		}

		public void setMapOutputKeyClass(Class<? extends Writable> mapOutputKeyClass) {
			this.mapOutputKeyClass = mapOutputKeyClass;
		}

		public Class<? extends Writable> getOutputKeyClass() {
			return outputKeyClass;
		}

		public void setOutputKeyClass(Class<? extends Writable> outputKeyClass) {
			this.outputKeyClass = outputKeyClass;
		}

		public Class<? extends Writable> getOutputValueClass() {
			return outputValueClass;
		}

		public void setOutputValueClass(Class<? extends Writable> outputValueClass) {
			this.outputValueClass = outputValueClass;
		}

		public Path getOutputPath() {
			return outputPath;
		}

		public void setOutputPath(Path outputPath) {
			this.outputPath = outputPath;
		}

		public void addInputPaths(Path... path) {
			inputPaths.addAll(Arrays.asList(path));
		}

		protected Job newJob(Configuration conf, String jobName) throws Exception {
			Job job = Job.getInstance(conf, jobName);
			job.setJarByClass(runClass);
			job.setMapperClass(mapperClass);

			job.setMapOutputKeyClass(mapOutputKeyClass);
			job.setMapOutputValueClass(mapOutputValueClass);
			job.setReducerClass(reducerClass);
			job.setOutputKeyClass(outputKeyClass);
			job.setOutputValueClass(outputValueClass);
			if (groupingComparatorClass != null) {
				job.setGroupingComparatorClass(groupingComparatorClass);
			}
			if (combinerClass != null) {
				job.setCombinerClass(combinerClass);
			}
			return job;
		}

		public void run(Configuration conf) throws Exception {
			conf.set("dfs.client.use.datanode.hostname", "true");
			FileSystem fileSystem = FileSystem.get(conf);
			Job targetJob = newJob(conf, taskName);
			if (fileSystem.exists(outputPath)) {
				fileSystem.delete(outputPath, true);
			}
			FileInputFormat.setInputPaths(targetJob, inputPaths.toArray(new Path[0]));
			FileOutputFormat.setOutputPath(targetJob, outputPath);
			boolean status = targetJob.waitForCompletion(true);
			if (!status) {
				throw new RuntimeException("Run Task Fail!");
			}
		}

	}

}
