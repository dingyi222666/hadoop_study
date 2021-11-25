package github.dingyi222666.hadoop.itemcf.demo1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;
import org.apache.log4j.Logger;

import github.dingyi222666.hadoop.service.HadoopService;
import github.dingyi222666.hadoop.service.HadoopService.HadoopTask;

public class Step6 {

	
	public static void main() throws Exception {
		runTask();
	}

	public static void runTask() throws Exception {

		HadoopService service = new HadoopService();

		HadoopTask task = new HadoopTask();

		task.setRunClass(Step6.class);

		task.setMapperClass(Step6Mapper.class);

		task.setReducerClass(Step6Reducer.class);

		task.setMapOutputKeyClass(RateWritable.class);

		task.setMapOutputValueClass(NullWritable.class);

		task.setOutputKeyClass(RateWritable.class);

		task.setOutputValueClass(NullWritable.class);

		task.setGroupingComparatorClass(RateGroup.class);

		task.setTaskName("step6");

		task.addInputPaths(service.wapperPath("/user/root/output/demo1/step5/part-r-00000"));

		task.setOutputPath(service.wapperPath("/user/root/output/demo1/step6"));

		Configuration configuration = service.getConfiguration();

		configuration.set("topN", "1");

		task.run(service.getConfiguration());
	}

	public static class Step6Mapper extends Mapper<LongWritable, Text, RateWritable, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, RateWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String[] splitArray = line.split("\t");

			String[] itemArray = splitArray[1].split(":");

			RateWritable writable = new RateWritable();

			writable.setRate(Double.parseDouble(itemArray[1]));

			writable.setUserId(splitArray[0]);

			writable.setItemId(itemArray[0]);

			context.write(writable, NullWritable.get());

		}

	}

	public static class RateGroup extends WritableComparator {

		public RateGroup() {
			super(RateWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			RateWritable aRateWritable = (RateWritable) a;
			RateWritable bRateWritable = (RateWritable) b;

			return aRateWritable.userId.compareTo(bRateWritable.userId);

		}

	}

	public static class Step6Reducer extends Reducer<RateWritable, NullWritable, RateWritable, NullWritable> {

		@Override
		protected void reduce(RateWritable arg0, Iterable<NullWritable> arg1,
				Reducer<RateWritable, NullWritable, RateWritable, NullWritable>.Context arg2)
				throws IOException, InterruptedException {

			// 这里有一个比较重要的地方 就是这个key不是一成不变的，
			// 并不是Map<Key,List<Value>> 而是 List<Pair<Key,Value>>
			// 如果做了分组操作，key不一样依旧被认为是一组，传输进去的时候就是List<Pair<Key,Value>>
			// 因为每调用一次的迭代器的next，其实也是在获取下一个Pair<Key,Value>
			// 这样的话因为key的不同，所以看起来也并不是同一个Key了
			// 参考链接：https://www.cnblogs.com/intsmaze/p/6737337.html

			int topN = Integer.parseInt(arg2.getConfiguration().get("topN"));

			int now = 0;

			Logger.getRootLogger().error("start for");
			for (NullWritable nullWritable : arg1) {

				Logger.getRootLogger().error(
						arg0.toString() + " " + arg0.getClass().getName() + '@' + Integer.toHexString(arg0.hashCode()));
				if (now > topN) {
					Logger.getRootLogger().error("for max");
				} else {

					arg2.write(arg0, nullWritable);

					now++;
				}
			}
			Logger.getRootLogger().error("end for");

		}
	}

	public static class RateWritable implements WritableComparable<RateWritable> {
		private String itemId;

		private String userId;
		private double rate;

		public String getItemId() {
			return itemId;
		}

		public void setItemId(String itemId) {
			this.itemId = itemId;
		}

		public String getUserId() {
			return userId;
		}

		public void setUserId(String userId) {
			this.userId = userId;
		}

		public double getRate() {
			return rate;
		}

		public void setRate(double rate) {
			this.rate = rate;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			userId = arg0.readUTF();
			itemId = arg0.readUTF();
			rate = arg0.readDouble();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			arg0.writeUTF(userId);
			arg0.writeUTF(itemId);
			arg0.writeDouble(rate);
		}

		@Override
		public int compareTo(RateWritable o) {
			int result = userId.compareTo(o.userId);

			// 同一个用户的话
			if (result == 0) {
				return Double.compare(o.rate, rate);
			}
			return result;
		}

		@Override
		public String toString() {
			return userId + " " + itemId + " " + rate;
		}

	}

}
