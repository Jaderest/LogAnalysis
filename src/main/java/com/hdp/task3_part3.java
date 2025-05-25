package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class task3_part3 {

    // Step 1: 构建浏览器类型的映射关系
    public static class BrowserTypeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, Integer> browserRank = new HashMap<>();
        private static final Pattern logPattern = Pattern.compile(
                "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"\\S+ (.*?) \\S+\" \\[(\\d+)\\] (\\d+) \"(.*?)\" \"(.*?)\"");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 直接从 HDFS 读取浏览器排序文件，并加载前10个浏览器
            String browserRankingFilePath = context.getConfiguration().get("browserRankingFilePath");
            if (browserRankingFilePath != null) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(browserRankingFilePath);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    int count = 0;
                    while ((line = reader.readLine()) != null && count < 10) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            String browser = parts[0];
                            int rank = Integer.parseInt(parts[1]);
                            browserRank.put(browser, rank);
                            count++;
                        }
                    }
                }
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String logLine = value.toString();
            // 使用正则提取浏览器类型和请求状态及数据传输量
            String status;
            String browser;
            String dataSent;
            if (logLine.isEmpty()) {
                return; // 跳过空行
            }
            Matcher matcher = logPattern.matcher(logLine);
            if (matcher.find()) {
                browser = classifyUserAgent(matcher.group(9)); // 提取浏览器类型
                status = matcher.group(6); // 提取状态码
                dataSent = matcher.group(7); // 提取传输数据量
            } else {
                return; // 如果不匹配，跳过该行
            }

            // 只处理前10个浏览器
            if (browserRank.containsKey(browser)) {
                // 输出键值对: 浏览器类型 -> 状态码: 错误/成功，数据量
                context.write(new Text(browser), new Text(status + ":" + dataSent));
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

            return "Other";
        }
    }

    public static class HashPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // 使用浏览器名称的哈希值来决定分区
            return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    // Step 2: Reducer 计算错误率与平均数据传输量
    public static class BrowserTypeReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalRequests = 0;
            int failedRequests = 0;
            float totalDataSent = 0;

            // 计算每种浏览器类型下不同状态码的错误率与平均数据传输量
            for (Text value : values) {
                String[] parts = value.toString().split(":");
                String status = parts[0];
                float dataSent = Float.parseFloat(parts[1]);

                totalRequests++;
                totalDataSent += dataSent;
                if (!status.startsWith("2")) { // 非200系列为失败请求
                    failedRequests++;
                }
            }

            float errorRate = totalRequests > 0 ? (float) failedRequests / totalRequests : 0.0f;
            float averageDataSent = totalRequests > 0 ? totalDataSent / totalRequests : 0.0f;

            // 输出浏览器类型和相关的状态码统计信息
            context.write(key, new Text("ErrorRate: " + errorRate + ", AvgDataSent: " + averageDataSent));
        }
    }

    // Step 3: 排序Mapper
    public static class SortMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");

            if (parts.length == 2) {
                try {
                    String errorRateStr = parts[0].split(":")[1].trim(); // 提取ErrorRate
                    float errorRate = Float.parseFloat(errorRateStr);

                    // 写入错误率和对应的浏览器及数据
                    context.write(new FloatWritable(-errorRate), new Text(line)); // 使用负值进行降序排序
                } catch (NumberFormatException e) {
                    // 如果解析失败，跳过该行
                    System.err.println("Skipping invalid line: " + line);
                }
            }
        }
    }

    // Step 4: 排序Reducer
    public static class SortReducer extends Reducer<FloatWritable, Text, Text, Text> {
        @Override
        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 直接输出所有排序过的数据
            for (Text value : values) {
                context.write(value, new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: task3_part3 <input path> <output path> <browser ranking file path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("browserRankingFilePath", args[2]); // 设置浏览器排序文件路径

        Job job = Job.getInstance(conf, "Log Analysis: High Risk Browser Types");
        job.setJarByClass(task3_part3.class);

        // 设置Mapper与Reducer类
        job.setMapperClass(BrowserTypeMapper.class);
        job.setReducerClass(BrowserTypeReducer.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(3);

        // 设置Map输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入与输出路径
        FileInputFormat.addInputPath(job, new Path(args[0])); // 日志文件输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

        // 执行第一个任务
        boolean success1 = job.waitForCompletion(true);
        if (!success1) {
            System.exit(1);
        }

        // 第二个MapReduce Job - 排序任务
        Job job2 = Job.getInstance(conf, "Log Analysis: Sorted Browser Types");
        job2.setJarByClass(task3_part3.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1])); // 第一个任务的输出路径作为输入
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_sorted")); // 排序后的输出路径

        // 执行排序任务
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
