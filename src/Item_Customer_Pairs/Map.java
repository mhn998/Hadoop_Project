package Item_Customer_Pairs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Pair, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] input = line.split(" ");

        for (int i = 0; i < input.length; i++) {
            for (int j = i + 1; j < input.length && !input[i].equals(input[j]); j++) {
                context.write(new Pair(input[i], input[j]), new IntWritable(1));
                context.write(new Pair(input[i], "*"), new IntWritable(1));
            }
        }
    }

}
