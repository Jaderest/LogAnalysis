package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.*;

public class task2_ver2 {

    public static class HourMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Pattern logPattern = Pattern.compile("\\[(\\d{2})/(\\w+)/(\\d{4}):(\\d{2})");

        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            Matcher matcher = logPattern.matcher(value.toString());
            if (matcher.find()) {
                String hour = matcher.group(3) + matcher.group(2) + matcher.group(1) + matcher.group(4);
                context.write(new Text(hour), new IntWritable(1));
            }
        }
    }

    public static class UserAgentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Pattern userAgentPattern = Pattern.compile("\"([^\"]*)\"\\s*$");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Matcher matcher = userAgentPattern.matcher(value.toString());
            if (matcher.find()) {
                String userAgent = matcher.group(1);
                String type = classifyUserAgent(userAgent);
                context.write(new Text(type), new IntWritable(1));
            }
        }

        private String classifyUserAgent(String ua) {
            ua = ua.toLowerCase();

            if (ua.contains("googlebot"))
                return "googlebot";
            if (ua.contains("baiduspider"))
                return "baiduspider";
            if (ua.contains("sogou"))
                return "sogou";
            if (ua.contains("mj12bot"))
                return "mj12bot";
            if (ua.contains("siteexplorer"))
                return "siteexplorer";
            if (ua.contains("youdaofeedfetcher"))
                return "youdaofeedfetcher";
            if (ua.contains("wordpress"))
                return "wordpress";
            if (ua.contains("dnspod"))
                return "dnspod";
            if (ua.contains("bingpreview"))
                return "Bing Preview";
            if (ua.contains("chrome"))
                return "Chrome";
            if (ua.contains("firefox"))
                return "Firefox";
            if ((ua.contains("msie") || ua.contains("trident")))
                return "Internet Explorer";
            if (ua.contains("safari") && !ua.contains("chrome"))
                return "Safari";
            if (ua.contains("opera") || ua.contains("presto"))
                return "Opera";
            if (ua.contains("ucbrowser"))
                return "UC Browser";
            if (ua.contains("metasr") || ua.contains("se 2.x"))
                return "Sogou Browser";
            if (ua.contains("maxthon"))
                return "Maxthon";

            if (ua.trim().equals("-") || ua.trim().equals(""))
                return "Unknown";
            return "Other";
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws java.io.IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static class HashPartitioner extends Partitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static class SwapMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                long count = Long.parseLong(parts[1]);
                context.write(new LongWritable(count), new Text(parts[0]));
            }
        }
    }

    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(LongWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b); // 降序排序
        }
    }

    public static class OutputReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Hourly Access Count");
        job1.setJarByClass(task2.class);
        job1.setMapperClass(HourMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        job1.setPartitionerClass(HashPartitioner.class);
        job1.setNumReduceTasks(4);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_hour"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "User Agent Count");
        job2.setJarByClass(task2.class);
        job2.setMapperClass(UserAgentMapper.class);
        job2.setCombinerClass(SumReducer.class);
        job2.setReducerClass(SumReducer.class);
        job2.setPartitionerClass(HashPartitioner.class);
        job2.setNumReduceTasks(4);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_agent"));
        job2.waitForCompletion(true);

        // === Job3: Sort Hour Access Count ===
        Job sortHour = Job.getInstance(conf, "Sort Hourly Count Descending");
        sortHour.setJarByClass(task2.class);
        sortHour.setMapperClass(SwapMapper.class);
        sortHour.setReducerClass(OutputReducer.class);
        sortHour.setSortComparatorClass(DescendingKeyComparator.class);
        sortHour.setMapOutputKeyClass(LongWritable.class);
        sortHour.setMapOutputValueClass(Text.class);
        sortHour.setOutputKeyClass(Text.class);
        sortHour.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(sortHour, new Path(args[1] + "_hour"));
        FileOutputFormat.setOutputPath(sortHour, new Path(args[1] + "_hour_sorted"));
        sortHour.waitForCompletion(true);

        // === Job4: Sort User Agent Count ===
        Job sortAgent = Job.getInstance(conf, "Sort UserAgent Count Descending");
        sortAgent.setJarByClass(task2.class);
        sortAgent.setMapperClass(SwapMapper.class);
        sortAgent.setReducerClass(OutputReducer.class);
        sortAgent.setSortComparatorClass(DescendingKeyComparator.class);
        sortAgent.setMapOutputKeyClass(LongWritable.class);
        sortAgent.setMapOutputValueClass(Text.class);
        sortAgent.setOutputKeyClass(Text.class);
        sortAgent.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(sortAgent, new Path(args[1] + "_agent"));
        FileOutputFormat.setOutputPath(sortAgent, new Path(args[1] + "_agent_sorted"));
        System.exit(sortAgent.waitForCompletion(true) ? 0 : 1);
    }
}
