package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.*;
import java.io.*;

public class task4_part1 {

    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Pattern logPattern = Pattern.compile(
                "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"\\S+ (.*?) \\S+\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\"");
        
        // 时间格式
        private static final SimpleDateFormat timeFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");

        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            Matcher matcher = logPattern.matcher(value.toString());
            if (matcher.find()) {
                String remoteAddr = matcher.group(1); // 获取IP地址
                String timeLocal = matcher.group(4); // 获取时间
                String request = matcher.group(5); // 获取请求内容
                String requestType = classifyRequest(request); // 获取请求类型
                String bytesSended = matcher.group(7); // 获取发送的字节数
                String userAgent = matcher.group(9); // 获取浏览器信息
                
                // 判断是PC端还是移动端
                String deviceType = classifyDevice(userAgent);
                
                // 判断访问时间段是白天还是晚上
                String timeOfDay = classifyTimeOfDay(timeLocal);

                // 根据IP地址作为Key，输出特征向量
                StringBuilder featureVector = new StringBuilder();
                featureVector.append("device:").append(deviceType).append(" ");
                featureVector.append("timeOfDay:").append(timeOfDay).append(" ");
                featureVector.append("bytesSent:").append(bytesSended).append(" ");
                featureVector.append("requestType:").append(requestType);
                
                context.write(new Text(remoteAddr), new Text(featureVector.toString()));
            }
        }

        // 判断设备类型：PC端或移动端
        private String classifyDevice(String userAgent) {
            if (userAgent.toLowerCase().contains("mobile")) {
                return "mobile";
            } else {
                return "pc";
            }
        }

        // 判断访问时间段：白天或晚上
        private String classifyTimeOfDay(String timeLocal) throws java.io.IOException {
            try {
                // 解析时间并判断时间段
                long timestamp = timeFormat.parse(timeLocal.split(" ")[0]).getTime();
                int hour = (int) ((timestamp / (1000 * 60 * 60)) % 24);
                return (hour >= 6 && hour <= 18) ? "day" : "night";
            } catch (ParseException e) {
                throw new IOException("Invalid time format: " + timeLocal);
            }
        }

        private String classifyRequest(String request) {
            if (request.contains("GET")) {
                return "GET";
            } else if (request.contains("POST")) {
                return "POST";
            } else if (request.contains("PUT")) {
                return "PUT";
            } else if (request.contains("DELETE")) {
                return "DELETE";
            } else {
                return "OTHER";
            }
        }


    }

    public static class LogReducer extends Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {
            //TODO：在特征向量中，添加统计用户请求次数的数据结构，并且在Reduce中输出
            //TODO：添加平均发送数据量
            // 输出每个用户的特征向量
            for (Text val : values) {
                context.write(val, NullWritable.get());
            }
        }
    }

    public static class IPPartitioner extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            try {
                String ip = key.toString();
                String firstOctetStr = ip.split("\\.")[0]; // 取第一个字段
                int firstOctet = Integer.parseInt(firstOctetStr);
                return (firstOctet < 127) ? 0 : 1;
            } catch (Exception e) {
                // 如果不是合法 IP，默认分到 Reducer 1
                return 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Behavior Feature Extraction");
        job.setJarByClass(task4_part1.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setPartitionerClass(IPPartitioner.class);
        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
