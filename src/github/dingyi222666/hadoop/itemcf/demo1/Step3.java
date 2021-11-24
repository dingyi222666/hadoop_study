package github.dingyi222666.hadoop.itemcf.demo1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import github.dingyi222666.hadoop.service.HadoopService;
import github.dingyi222666.hadoop.service.HadoopService.HadoopTask;

/**
 * @author dingyi
 * 计算用户对物品打分的矩阵
 */
public class Step3 {
	public static void main() throws Exception {
		runTask();
	}

	public static void runTask() throws Exception {

		HadoopService service = new HadoopService();

		HadoopTask task = new HadoopTask();

		task.setMapperClass(Step3Mapper.class);

		task.setMapOutputKeyClass(Text.class);

		task.setMapOutputValueClass(Text.class);

		task.setRunClass(Step3.class);

		task.setOutputKeyClass(Text.class);

		task.setOutputValueClass(Text.class);

		task.setReducerClass(Step3Reducer.class);

		task.setTaskName("step3");

		task.addInputPaths(service.wapperPath("/user/root/output/demo1/step1/part-r-00000"));

		task.setOutputPath(service.wapperPath("/user/root/output/demo1/step3"));

		task.run(service.getConfiguration());
	}

	/**
	 * output (Text(userId),Text(itemId,scope))
	 */
	public static class Step3Mapper extends Mapper<LongWritable, Text, Text, Text> {

		private static Text tmp1 = new Text();

		private static Text tmp2 = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] splitArray = value.toString().split("\t");
			
			
			String[] scopeArray = splitArray[1].split(",");
			
			for (String string : scopeArray) {
				
				String[] scopeArray2 = string.split(":") ;
				
				tmp1.set(scopeArray2[0]);
				
				tmp2.set(splitArray[0]+":"+scopeArray2[1]);
				
				
				context.write(tmp1, tmp2);
				
			
			}
			
			
			
		}
	}

	public static class Step3Reducer extends Reducer<Text, Text, Text, Text> {

		
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {

			
			for (Text text : arg1) {
				
				arg2.write(arg0, text);

			}
			
			
		}
	}
}
