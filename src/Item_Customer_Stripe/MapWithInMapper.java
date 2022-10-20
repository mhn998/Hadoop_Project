package Item_Customer_Stripe;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapWithInMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] input = line.split(" ");

        for (int i = 0; i < input.length; i++) {
            MapWritable mapWritable = new MapWritable();
            for (int j = i + 1; j < input.length && !input[i].equals(input[j]); j++) {
                if (mapWritable.get(new Text(input[j])) == null) {
                    mapWritable.put(new Text(input[j]), new IntWritable(1));
                } else {
                    IntWritable inWritable = (IntWritable) mapWritable
                            .get(new Text(input[j]));
                    mapWritable.put(new Text(input[j]), new IntWritable(
                            inWritable.get() + 1));
                }
            }
            context.write(new Text(input[i]), mapWritable);
        }
    }
}
