import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageInMapper {

    public static class Map extends Mapper<LongWritable, Text, Text, RequestSizeAndQuantity> {
        HashMap<String,RequestSizeAndQuantity> H = new HashMap<>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] tokens = line.split(" ");

            String ipAddressString = getIpAddress(tokens[0]);
            if(ipAddressString != null)
            {
                if(H.containsKey(ipAddressString))
                {
                    RequestSizeAndQuantity temp = H.get(ipAddressString);
                    H.put(ipAddressString,new RequestSizeAndQuantity(new IntWritable(Integer.parseInt(tokens[tokens.length-1])+temp.getRequestSize().get()),new IntWritable(temp.getCount().get()+1)));
                }
                else {
                    H.put(ipAddressString, new RequestSizeAndQuantity(new IntWritable(tokens[tokens.length-1].equals("-")?0:Integer.parseInt(tokens[tokens.length-1])), new IntWritable(1)));
                }
//                context.write(new Text(ipAddressString),new IntWritable(Integer.parseInt(tokens[tokens.length-1])));//new Text(+","+1)//new RequestSizeAndQuantity(new IntWritable(getRequestSize(line)), new IntWritable(1))
            }


        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, RequestSizeAndQuantity>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for(java.util.Map.Entry<String,RequestSizeAndQuantity> e : H.entrySet()) {
                context.write(new Text(e.getKey()), e.getValue());
            }
        }
    }

    public static class Reduce extends Reducer<Text, RequestSizeAndQuantity, Text, DoubleWritable> {
        HashMap<String,RequestSizeAndQuantity> H = new HashMap<>();

        public void reduce(Text key, Iterable<RequestSizeAndQuantity> values, Context context)
                throws IOException, InterruptedException {

//			int sum = 0;
//			int count = 0;
            for (RequestSizeAndQuantity val : values) {
//				String[] split = val.split(",");
//				sum+=val.get();
//				count++;
                if(H.containsKey(key.toString()))
                {
                    RequestSizeAndQuantity temp = H.get(key.toString());
                    H.put(key.toString(),new RequestSizeAndQuantity(new IntWritable(val.getRequestSize().get()+temp.getRequestSize().get()),new IntWritable(val.getCount().get()+temp.getCount().get())));
                }
                else {
                    H.put(key.toString(), new RequestSizeAndQuantity(new IntWritable(val.getRequestSize().get()), new IntWritable(val.getCount().get())));
                }
            }


        }

        @Override
        protected void cleanup(Reducer<Text, RequestSizeAndQuantity, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for(java.util.Map.Entry<String, RequestSizeAndQuantity> e : H.entrySet())
            {
                context.write(new Text(e.getKey()),new DoubleWritable((double)e.getValue().getRequestSize().get()/e.getValue().getCount().get()));
            }
        }
    }

    static String getIpAddress(String ipString){
        String IPADDRESS_PATTERN =
                "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

        Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
        Matcher matcher = pattern.matcher(ipString);
        if (matcher.find()) {
            return matcher.group();
        } else{
            return null;
        }
    }

    static int getRequestSize(String str)
    {
        return Integer.parseInt(str.replace("\"",""));
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Average");
        job.setJarByClass(Average.class);

        //Reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //Mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RequestSizeAndQuantity.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }


}