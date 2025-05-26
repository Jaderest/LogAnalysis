package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.*;

// Task3 Part2
public class task3_part2 {

    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Pattern logPattern = Pattern.compile(
                "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"\\S+ (.*?) \\S+\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\"");

        private static final SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH");

        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            Matcher matcher = logPattern.matcher(value.toString());
            if (matcher.find()) {
                String status = matcher.group(6); // 请求状态码
                String timeLocal = matcher.group(4); // 获取访问时间
                try {
                    // 将访问时间转换为小时
                    Date date = sdf.parse(timeLocal);
                    String hour = new SimpleDateFormat("HH").format(date);

                    // 如果请求状态是失败（非200系列），则认为是错误
                    int isError = (status.startsWith("2")) ? 0 : 1;

                    // 输出时间段（小时）和错误计数
                    context.write(new Text(hour), new IntWritable(isError));
                } catch (Exception e) {
                    // 忽略解析错误的日志行
                }
            }
        }
    }

    public static class LogReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalRequests = 0;
            int failedRequests = 0;

            for (IntWritable value : values) {
                totalRequests++;
                if (value.get() == 1) {
                    failedRequests++;
                }
            }

            float errorRate = totalRequests > 0 ? (float) failedRequests / totalRequests : 0.0f;
            context.write(key, new FloatWritable(errorRate));
        }
    }

    public static class LogPatitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            int hour = Integer.parseInt(key.toString());
            // 将小时分为两组：8-22点 和 0-7点、23点
            if (hour >= 8 && hour <= 22) {
                return 0; // 白天时间段
            } else {
                return 1; // 夜间时间段
            }
        }
    }

    // 新的Mapper用于排序
    public static class SortMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                try {
                    // 错误率在字段1
                    float errorRate = -Float.parseFloat(fields[1]);
                    context.write(new FloatWritable(errorRate), new Text(fields[0]));
                } catch (NumberFormatException e) {
                    // 忽略格式错误的行
                }
            }
        }
    }

    // 新的Reducer用于排序
    public static class SortReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                float errorRate = -key.get(); // 取负值以实现降序排序
                context.write(value, new FloatWritable(errorRate));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log Analysis: Hourly Error Rate");
        job.setJarByClass(task3_part2.class);

        // 设置Mapper与Reducer类
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setPartitionerClass(LogPatitioner.class);
        job.setNumReduceTasks(2); // 设置两个Reducer

        // 设置Map输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 设置输入与输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 执行任务并返回状态
        boolean success = job.waitForCompletion(true);
        if (success) {
            // 执行排序的MapReduce任务
            Job sortJob = Job.getInstance(conf, "Sort Error Rate");
            sortJob.setJarByClass(task3_part2.class);

            // 设置新的Mapper和Reducer
            sortJob.setMapperClass(SortMapper.class);
            sortJob.setReducerClass(SortReducer.class);

            // 设置Map输出的key和value类型
            sortJob.setMapOutputKeyClass(FloatWritable.class);
            sortJob.setMapOutputValueClass(Text.class);

            // 设置最终输出的key和value类型
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(FloatWritable.class);

            // 设置输入与输出路径
            FileInputFormat.addInputPath(sortJob, new Path(args[1]));
            FileOutputFormat.setOutputPath(sortJob, new Path(args[1] + "_sorted"));

            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        }
    }
}
