package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

import java.util.regex.*;


public class task3_part1 {
    public static class LogMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private static final Pattern logPattern = Pattern.compile(
                "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"\\S+ (.*?) \\S+\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\"");

        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            Matcher matcher = logPattern.matcher(value.toString());
            if (matcher.find()) {
                String status = matcher.group(6); // 获取请求状态码
                String bodyBytesSent = matcher.group(7); // 获取传输数据量

                try {
                    // 转换传输数据量为float
                    float dataSent = Float.parseFloat(bodyBytesSent);
                    context.write(new Text(status), new FloatWritable(dataSent));
                } catch (NumberFormatException e) {
                    // 如果转换失败，则忽略
                }
            }
        }
    }

    public static class LogReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable value : values) {
                sum += value.get();
                count++;
            }
            float average = count > 0 ? sum / count : 0;
            context.write(key, new FloatWritable(average));
        }
    }

    // 自定义分区器，将数据分为两组：正常状态码与失败状态码
    public static class StatusPartitioner extends Partitioner<Text, FloatWritable> {
        @Override
        public int getPartition(Text key, FloatWritable value, int numPartitions) {
            String status = key.toString();
            if (status.equals("200") || status.equals("201") || status.equals("202") || status.equals("204")) {
                return 0; // 正常状态码
            } else if (status.equals("400") || status.equals("401") || status.equals("403") || status.equals("404") || status.equals("500")) {
                return 1; // 错误状态码
            } else {
                return 1; // 其他状态码归为错误状态
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log Analysis: Status vs Data Transmission");
        job.setJarByClass(task3_part1.class);

        // 设置Mapper与Reducer类
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setPartitionerClass(StatusPartitioner.class);
        job.setNumReduceTasks(2); // 设置两个Reducer

        // 设置Map输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        // 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 设置输入与输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 执行任务并返回状态
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
