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
        private static final Pattern userAgentPattern = Pattern.compile("\"([^\"]*)\"\\s*$");

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
            String browser = extractBrowser(logLine);
            String status = extractStatus(logLine);
            String dataSent = extractDataSent(logLine);

            // 只处理前10个浏览器
            if (browserRank.containsKey(browser)) {
                // 输出键值对: 浏览器类型 -> 状态码: 错误/成功，数据量
                context.write(new Text(browser), new Text(status + ":" + dataSent));
            }
        }

        private String extractBrowser(String logLine) {
            // 从日志行中提取浏览器信息 (简化为正则表达式匹配)
            Matcher matcher = userAgentPattern.matcher(logLine);
            if (matcher.find()) {
                String userAgent = matcher.group(1);
                return classifyUserAgent(userAgent);
            }
            return "Unknown";
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

        private String extractStatus(String logLine) {
            String statusPattern = "\\[([0-9]+)\\]";
            Pattern pattern = Pattern.compile(statusPattern);
            Matcher matcher = pattern.matcher(logLine);
            if (matcher.find()) {
                return matcher.group(1); // 提取状态码
            }
            return "0"; // 默认返回 0
        }

        private String extractDataSent(String logLine) {
            String dataPattern = "\\s([0-9]+)\\s\"";
            Pattern pattern = Pattern.compile(dataPattern);
            Matcher matcher = pattern.matcher(logLine);
            if (matcher.find()) {
                return matcher.group(1); // 提取传输数据量
            }
            return "0"; // 默认返回 0
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

        // 设置Map输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入与输出路径
        FileInputFormat.addInputPath(job, new Path(args[0])); // 日志文件输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

        // 执行任务并返回状态
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
