package github.dingyi222666.hadoop.itemcf.demo1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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
 * 计算物品同现矩阵
 */
public class Step2 {

	public static void main() throws Exception {
		runTask();
	}
	
	
	public static void runTask() throws Exception {
		
		HadoopService service = new HadoopService();
		
		HadoopTask task = new HadoopTask();
		
		task.setMapperClass(Step2Mapper.class);
	
		task.setMapOutputKeyClass(Text.class);
	
		task.setMapOutputValueClass(IntWritable.class);
		
		task.setRunClass(Step2.class);
		
		task.setOutputKeyClass(Text.class);
		
		task.setOutputValueClass(IntWritable.class);
		
		task.setReducerClass(Step2Reducer.class);
		
		task.setTaskName("step2");
		
		task.addInputPaths(service.wapperPath("/user/root/output/demo1/step1/part-r-00000"));
		
		task.setOutputPath(service.wapperPath("/user/root/output/demo1/step2"));
		
	
		task.run(service.getConfiguration());
	}
	
	
	/**
	 * output (Text(userId),Text(itemId,scope)) 
	 */
	public static class Step2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static Text tmp1 = new Text();
		
		private static IntWritable tmp2 = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] userArray = value.toString().split("\t");
			
			if (userArray.length!=2) {
				return;
			}
			
			String itemString = userArray[1];
			
			List<String> itemList = new ArrayList<>();
			
			StringTokenizer tokenizer  = new StringTokenizer(itemString,",");
			
			while (tokenizer.hasMoreTokens()) {
				itemList.add(tokenizer.nextToken());
			}
			
			for (String item1 : itemList) {
				for (String item2 : itemList) {
					String itemId = item1.split(":")[0]+","+item2.split(":")[0];
					tmp1.set(itemId);
					context.write(tmp1, tmp2);
				}
			}
			
			itemList.clear();
		}
	}
	
	
	public static class Step2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		
		private static IntWritable tmp2 = new IntWritable();
		
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1, Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			
			int count = 0;
					
			for (IntWritable intWritable : arg1) {
				count += intWritable.get();
			}
			
			tmp2.set(count);
			
			arg2.write(arg0, tmp2);
			
		}
	}
}
