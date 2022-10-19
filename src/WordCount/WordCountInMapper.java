import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountInMapper {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Logger logger = Logger.getLogger(WordCountInMapper.class.getName());
		private Text word = new Text();


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			HashMap<String, Integer> inMapper = new HashMap<>();

			
			logger.info("----- I  mapper Combiner Output ------ ");
			
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());

				if (inMapper.containsKey(word.toString())) {
					int sum =inMapper.get(word.toString()) + 1;
					inMapper.put(word.toString(), sum);
				} else {
					inMapper.put(word.toString(), 1);
				}
			}

			for (Entry<String, Integer> entry : inMapper.entrySet()) {
				String word = entry.getKey();
				int count = entry.getValue();
				logger.info(" --- Key : "+word+ "    val : "+count);
				context.write(new Text(word), new IntWritable(count));
			}

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "WordCountInMapper");
		job.setJarByClass(WordCountInMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		



		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}