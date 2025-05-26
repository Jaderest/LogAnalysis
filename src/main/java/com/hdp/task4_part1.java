package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.*;
import java.io.*;

public class task4_part1 {

    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Pattern logPattern = Pattern.compile(
                "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"(\\S+) (.*?) \\S+\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\"");

        private static final SimpleDateFormat timeFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");

        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            Matcher matcher = logPattern.matcher(value.toString());
            if (matcher.find()) {
                String remoteAddr = matcher.group(1); // 获取IP地址
                String timeLocal = matcher.group(4); // 获取时间
                String requestType = matcher.group(5); // 获取请求种类
                String bytesSended = matcher.group(8); // 获取发送的字节数
                String userAgent = matcher.group(10); // 获取浏览器信息

                // 判断设备类型：PC端或移动端
                String deviceType = classifyDevice(userAgent);

                // 判断访问时间段：白天或晚上
                String timeOfDay = classifyTimeOfDay(timeLocal);

                // 根据IP地址作为Key，输出特征向量
                StringBuilder featureVector = new StringBuilder();
                featureVector.append("device:").append(deviceType).append("\t");
                featureVector.append("timeOfDay:").append(timeOfDay).append("\t");
                featureVector.append("requestType:").append(requestType).append("\t");

                // 将字节数和请求次数传递给Reducer
                featureVector.append("bytesSent:").append(bytesSended);

                // 输出
                context.write(new Text(remoteAddr), new Text(featureVector.toString() + "\t1")); // 1为请求次数
            }
        }

        // 判断设备类型: PC端或移动端
        // 移动端包含 "mobile" 字符串，PC端则不包含
        // pc为1，mobile为0
        private String classifyDevice(String userAgent) {
            if (userAgent.toLowerCase().contains("mobile")) {
                return "0";
            } else {
                return "1";
            }
        }

        // 判断访问时间段：白天或晚上
        private String classifyTimeOfDay(String timeLocal) throws java.io.IOException {
            try {
                long timestamp = timeFormat.parse(timeLocal.split(" ")[0]).getTime();
                int hour = (int) ((timestamp / (1000 * 60 * 60)) % 24);
                return (hour >= 6 && hour <= 18) ? "day" : "night";
            } catch (ParseException e) {
                throw new IOException("Invalid time format: " + timeLocal);
            }
        }
    }

    public static class LogReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {

            // 初始化统计数据
            long totalRequestsDay = 0;
            long totalBytesSentDay = 0;
            long totalRequestsNight = 0;
            long totalBytesSentNight = 0;

            // 存储特征信息
            String deviceType = "";

            // 用于统计每种请求类型的次数
            Map<String, Integer> requestTypeCount = new HashMap<>();

            // 处理每个用户的行为记录
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length < 4) {
                    continue; // 如果格式不正确，跳过
                }

                // 解析设备类型
                if (deviceType.isEmpty()) {
                    deviceType = parts[0].split(":")[1]; // 设备类型
                }

                // 解析请求类型并进行统计
                String requestType = parts[2].split(":")[1]; // 请求类型
                requestTypeCount.put(requestType, requestTypeCount.getOrDefault(requestType, 0) + 1);

                // 解析字节数和时间段
                long bytesSent = Long.parseLong(parts[3].split(":")[1]);
                String timeOfDay = parts[1].split(":")[1]; // 时间段：day 或 night

                // 根据时间段累加请求次数和字节数
                if ("day".equals(timeOfDay)) {
                    totalRequestsDay += 1;
                    totalBytesSentDay += bytesSent;
                } else if ("night".equals(timeOfDay)) {
                    totalRequestsNight += 1;
                    totalBytesSentNight += bytesSent;
                }
            }

            // 计算每个时间段的平均字节数
            double avgBytesSentDay = totalRequestsDay > 0 ? (double) totalBytesSentDay / totalRequestsDay : 0;
            double avgBytesSentNight = totalRequestsNight > 0 ? (double) totalBytesSentNight / totalRequestsNight : 0;

            // 计算总请求数和加权平均字节数
            long totalRequests = totalRequestsDay + totalRequestsNight;
            double totalAvgBytesSent = totalRequests > 0
                    ? (totalRequestsDay * avgBytesSentDay + totalRequestsNight * avgBytesSentNight) / totalRequests
                    : 0;

            // 构建请求类型的字符串输出，按格式 [GET:10, POST:5, PUT:2, DELETE:1, OTHER:3]
            StringBuilder requestTypeStringBuilder = new StringBuilder();
            for (Map.Entry<String, Integer> entry : requestTypeCount.entrySet()) {
                if (requestTypeStringBuilder.length() > 0) {
                    requestTypeStringBuilder.append(", ");
                }
                requestTypeStringBuilder.append(entry.getKey()).append(":").append(entry.getValue());
            }

            // 构建最终的特征向量输出
            StringBuilder featureVector = new StringBuilder();
            featureVector.append("deviceType:").append(deviceType).append("\t");
            featureVector.append("requestTypes:").append("[").append(requestTypeStringBuilder.toString()).append("]")
                    .append("\t");
            featureVector.append("dayRequests:").append(totalRequestsDay).append("\t");
            featureVector.append("dayAvgBytesSent:").append(avgBytesSentDay).append("\t");
            featureVector.append("nightRequests:").append(totalRequestsNight).append("\t");
            featureVector.append("nightAvgBytesSent:").append(avgBytesSentNight).append("\t");
            featureVector.append("totalRequests:").append(totalRequests).append("\t");
            featureVector.append("totalAvgBytesSent:").append(totalAvgBytesSent);

            // 输出最终结果
            context.write(key, new Text(featureVector.toString()));
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
