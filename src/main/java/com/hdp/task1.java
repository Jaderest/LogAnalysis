package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.regex.*;

public class task1 {
    public static class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private static final Pattern logPattern = Pattern.compile(
                "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"\\S+ (.*?) \\S+\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\""
        );

        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            Matcher matcher = logPattern.matcher(value.toString());
            if (matcher.find()) {
                StringBuilder sb = new StringBuilder();
                sb.append("remote_addr:").append(matcher.group(1)).append("\n");
                sb.append("remote_user:").append(matcher.group(2)).append(" ").append(matcher.group(3)).append("\n");
                sb.append("time_local:").append(matcher.group(4)).append("\n");
                sb.append("request:").append(matcher.group(5)).append("\n");
                sb.append("status:").append(matcher.group(6)).append("\n");
                sb.append("body_bytes_sent:").append(matcher.group(7)).append("\n");
                sb.append("http_referer:\"").append(matcher.group(8)).append("\"\n");
                sb.append("http_user_agent:\"").append(matcher.group(9)).append("\"\n");

                context.write(new Text(sb.toString()), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(task1.class);
        job.setMapperClass(LogMapper.class);
        job.setNumReduceTasks(0); // Mapper-only job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

