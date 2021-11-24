package github.dingyi222666.hadoop.itemcf.demo1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import github.dingyi222666.hadoop.service.HadoopService;
import github.dingyi222666.hadoop.service.HadoopService.HadoopTask;

/**
 * @author dingyi 计算用户对物品打分的矩阵
 */
public class Step5 {
	public static void main() throws Exception {
		runTask();
	}

	public static void runTask() throws Exception {

		HadoopService service = new HadoopService();

		HadoopTask task = new HadoopTask();

		task.setMapperClass(Step5Mapper.class);

		task.setReducerClass(Step5Reducer.class);

		task.setMapOutputKeyClass(Text.class);

		task.setMapOutputValueClass(Text.class);

		task.setRunClass(Step3.class);

		task.setOutputKeyClass(Text.class);

		task.setOutputValueClass(Text.class);

		task.setTaskName("step5");

		task.addInputPaths(service.wapperPath("/user/root/output/demo1/step4/part-r-00000"),
				service.wapperPath("/user/root/output/demo1/step3/part-r-00000"));

		task.setOutputPath(service.wapperPath("/user/root/output/demo1/step5"));

		task.run(service.getConfiguration());
	}

	public static class Step5Mapper extends Mapper<LongWritable, Text, Text, Text> {

		private static Text tmp1 = new Text();

		private static Text tmp2 = new Text();

		private boolean flag = false;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String name = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();

			if (name.equals("step3")) {
				flag = true;
			} else {
				flag = false;
			}

			
			
		}

		/**
		 * 直接按 (user,item) 这样的去聚合 更简单
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			Logger.getRootLogger().error("line:"+line+" flag:"+flag);
			
			
			String[] splitArray = line.split("\t");

			String[] itemArray = splitArray[1].split(":");

			if (flag == false) {

				// user / item
				tmp1.set(splitArray[0] + "," + itemArray[0]);

				// value
				tmp2.set(itemArray[1]);

				context.write(tmp1, tmp2);
			} else {

				// user / item
				tmp1.set(itemArray[0] + "," + splitArray[0]);

				// value
				tmp2.set("已评分项目");

				Logger.getRootLogger().error("logger:"+itemArray[0]+","+splitArray[0]);
				
				context.write(tmp1, tmp2);
			}

		}

	}

	public static class Step5Reducer extends Reducer<Text, Text, Text, Text> {

		private static Text tmp1 = new Text();

		private static Text tmp2 = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			
			double sum = 0;

			boolean isRated = false;

			
		   
			for (Text value : arg1) {
				String valueString = value.toString();
				Logger.getRootLogger().error(arg0.toString()+ " "+valueString);
				// 过滤掉已经评分的项目
				if ("已评分项目".equals(valueString)) {
					isRated = true;
					continue;
				}
				double doubleValue = Double.parseDouble(valueString);
				sum += doubleValue;
			}

			String[] itemArray = arg0.toString().split(",");

			Logger.getRootLogger().error(Arrays.toString(itemArray)+" "+isRated);

			tmp1.set(itemArray[0]);

			tmp2.set(itemArray[1] + ":" + sum);

			if (isRated == false) {
				arg2.write(tmp1, tmp2);
			}

		}

	}

}
