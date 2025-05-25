package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.*;

public class task2 {

    public static class HourMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Pattern logPattern = Pattern.compile("\\[(\\d{2})/(\\w+)/(\\d{4}):(\\d{2})");

        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            Matcher matcher = logPattern.matcher(value.toString());
            if (matcher.find()) {
                String hour = matcher.group(3) + matcher.group(2) + matcher.group(1) + matcher.group(4); // YYYYMMMDDHH
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

            if ((ua.contains("googlebot")) ||
                    (ua.contains("baiduspider")) ||
                    (ua.contains("sogou") && ua.contains("spider")) ||
                    (ua.contains("mj12bot")) ||
                    (ua.contains("siteexplorer")) ||
                    (ua.contains("youdaofeedfetcher")) ||
                    (ua.contains("wordpress")) ||
                    (ua.contains("dnspod")))
                return "Bot";

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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // job 1: 每小时访问统计
        Job job1 = Job.getInstance(conf, "Hourly Access Count");
        job1.setJarByClass(task2.class);
        job1.setMapperClass(HourMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_hour"));
        job1.waitForCompletion(true);

        // job 2: 用户端统计
        Job job2 = Job.getInstance(conf, "User Agent Count");
        job2.setJarByClass(task2.class);
        job2.setMapperClass(UserAgentMapper.class);
        job2.setCombinerClass(SumReducer.class);
        job2.setReducerClass(SumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_agent"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
