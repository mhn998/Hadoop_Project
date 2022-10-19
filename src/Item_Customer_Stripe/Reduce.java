package Item_Customer_Stripe;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, MapWritable, Text, StripeValue> {

	@Override
	protected void reduce(Text item, Iterable<MapWritable> values,
			Reducer<Text, MapWritable, Text, StripeValue>.Context context)
			throws IOException, InterruptedException {
		
		MapWritable sumMap = new MapWritable();
		double total = 0.0;
		for (MapWritable v : values) {
			for (Entry<Writable, Writable> entry : v.entrySet()) {
				if (sumMap.containsKey(entry.getKey())) {
					int t = ((IntWritable) sumMap.get(entry.getKey())).get();
					sumMap.put(entry.getKey(), new IntWritable(t
							+ ((IntWritable) entry.getValue()).get()));
				} else {
					sumMap.put(entry.getKey(), entry.getValue());
				}
				total += ((IntWritable) entry.getValue()).get();
			}
		}
		
		StripeValue finalMap = new StripeValue();
		DecimalFormat df = new DecimalFormat("#.#####");
		for (Entry<Writable, Writable> entry : sumMap.entrySet()) {
			double relativeFr = ((IntWritable) entry.getValue()).get() / total;
			finalMap.put(entry.getKey(), new DoubleWritable(Double.parseDouble(df.format(relativeFr))));
		}
		context.write(item, finalMap);
	}
}
