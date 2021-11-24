package github.dingyi222666.hadoop.itemcf.demo1;


import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import github.dingyi222666.hadoop.service.HadoopService;
import github.dingyi222666.hadoop.service.HadoopService.HadoopTask;

/**
 * @author dingyi
 * 整理成用户 ->评分列表
 */
public class Step1 {
	
	
	public static void main() throws Exception {
		runTask();
	}
	
	
	public static void runTask() throws Exception {
		
		HadoopService service = new HadoopService();
		
		HadoopTask task = new HadoopTask();
		
		task.setMapperClass(Step1Mapper.class);
	
		task.setMapOutputKeyClass(Text.class);
	
		task.setMapOutputValueClass(Text.class);
		
		task.setRunClass(Step1.class);
		
		task.setOutputKeyClass(Text.class);
		
		task.setOutputValueClass(Text.class);
		
		task.setReducerClass(Step1Reducer.class);
		
		task.setTaskName("step1");
		
		task.addInputPaths(service.wapperPath("/user/root/input/demo1/input.txt"));
		
		task.setOutputPath(service.wapperPath("/user/root/output/demo1/step1"));
		
	
		task.run(service.getConfiguration());
	}
	
	
	/**
	 * output (Text(userId),Text(itemId,scope)) 
	 */
	public static class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private static Text tmp1 = new Text();
		
		private static Text tmp2 = new Text();
		
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer tokenizer = new StringTokenizer(value.toString()," \t\n\r\f,");
			
			String[] item  = new String[3];
			
			int index = 0;
			
			while (tokenizer.hasMoreTokens()) {
				
				String text =tokenizer.nextToken();
				
				item[index] = text;
				
				index++;	
			}
			
			tmp1.set(item[0]);
		
			tmp2.set(item[1]+":"+item[2]);
			
			context.write(tmp1,tmp2);
			
		}
	}
	
	
	public static class Step1Reducer extends Reducer<Text, Text, Text, Text> {
		
		private static Text tmp1 = new Text();
		
		
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			
			StringBuilder builder = new StringBuilder();
			
			for (Text text : arg1) {
				builder.append(text.toString());
				builder.append(",");
			}
			builder.deleteCharAt(builder.length()-1);
			tmp1.set(builder.toString());
			
			arg2.write(arg0, tmp1);
			
		}
		
		
	}
	
}
