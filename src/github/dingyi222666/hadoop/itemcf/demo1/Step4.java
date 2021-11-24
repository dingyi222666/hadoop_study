package github.dingyi222666.hadoop.itemcf.demo1;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;

import github.dingyi222666.hadoop.service.HadoopService;
import github.dingyi222666.hadoop.service.HadoopService.HadoopTask;

public class Step4 {

	public static void main() throws Exception {
		runTask();
	}

	public static void runTask() throws Exception {

		HadoopService service = new HadoopService();

		HadoopTask task = new HadoopTask();

		task.setTaskName("step4");

		task.setMapOutputKeyClass(Text.class);

		task.setMapOutputValueClass(Text.class);

		task.setMapperClass(Step4Mapper.class);

		task.setOutputKeyClass(Text.class);

		task.setOutputValueClass(Text.class);

		task.setRunClass(Step4.class);

		task.setReducerClass(Step4Reducer.class);

		task.addInputPaths(service.wapperPath("/user/root/output/demo1/step3/part-r-00000"),
				service.wapperPath("/user/root/output/demo1/step2/part-r-00000"));

		task.setOutputPath(service.wapperPath("/user/root/output/demo1/step4"));

		task.run(service.getConfiguration());
	}

	/**
	 * 
	 * @author dingyi 这里比较重要 读完之后会聚合出来 注意输出key是用户id 输入key就是当前商品（行）
	 */
	public static class Step4Reducer extends Reducer<Text, Text, Text, Text> {

		private static Text tmp1 = new Text();
		
		private static Text tmp2 = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2) {

			Map<String, String> userVectorMap = new HashMap<String, String>();

			Map<String, String> itemVertorMap = new HashMap<String, String>();
			
			
	        
			
			
			for (Text text : arg1) {
				String line = text.toString();
				String[] splitArray = line.split(":");
				if (line.indexOf("A") != -1) {
					userVectorMap.put(splitArray[1], splitArray[2]);
				} else {
					itemVertorMap.put(splitArray[1], splitArray[2]);
				}

			}

			itemVertorMap.forEach( (itemKey,itemValue) -> {
				// (arg0,itemKey ) (itemValue)
				// (行，列) (同现次数)
				userVectorMap.forEach( (userKey,userValue) -> {
					
					int itemValueInt = Integer.parseInt(itemValue);
					
					double scope = Double.parseDouble(userValue);
					
					double result = itemValueInt * scope;
					
					tmp1.set(userKey);
					
					tmp2.set(itemKey+":"+Double.toString(result));
					
						try {
							arg2.write(tmp1,tmp2);
						} catch (IOException | InterruptedException e) {
							ByteOutputStream stream = new ByteOutputStream(1024);
							PrintWriter writer = new PrintWriter(stream);
							e.printStackTrace(writer);
							Logger.getRootLogger().error(new String(stream.getBytes()));
							
							writer.close();
							stream.close();
							
						}
					
					
				});
				
			});

		}

	}

	/**
	 * 
	 * @author dingyi 实现矩阵乘法的聚合 注意 key输出的是对应矩阵的位置 其实这里我们有多少用户 多少物品都是完全未知的
	 *         这里把物品作为key( value则为 (矩阵类型,((B) (用户,物品，用户评分)) ( (A) (对应同现物品,同现次数)))
	 */
	public static class Step4Mapper extends Mapper<LongWritable, Text, Text, Text> {

		private String type = "default";

		private Text tmp1 = new Text();

		private Text tmp2 = new Text();

		/**
		 * 处理读取的矩阵类型
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			FileSplit split = (FileSplit) context.getInputSplit();

			// 这里取parent是为了获取父目录文件名
			String path = split.getPath().getParent().getName();
			Logger.getRootLogger().error(path);
			if (path.equals("step3")) {
				// 评分矩阵
				type = "A";
				Logger.getRootLogger().error("insert a now!");
			} else {
				// 同现矩阵
				type = "B";
			}

		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			Logger.getRootLogger().error(type);

			// 评分矩阵
			if (type.equals("A")) {
				String[] split = line.split("\t");

				String itemId = split[0];

				tmp1.set(itemId);

				tmp2.set("A:" + split[1]);

				context.write(tmp1, tmp2);

			} else if (type.equals("B")) {

				String[] split = line.split("\t");

				String[] itemArray = split[0].split(",");

				if (itemArray.length > 0) {
					Logger.getRootLogger().error(Arrays.toString(split) + " " + split.length + " " + itemArray.length);
				}
				tmp1.set(itemArray[0]);

				tmp2.set("B:" + itemArray[1] + ":" + split[1]);

				context.write(tmp1, tmp2);

			}

		}

	}

}
