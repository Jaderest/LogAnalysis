package com.hdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class task4_part2_Iterative {

    public static void main(String[] args) throws Exception {
        // 检查参数数量
        if (args.length != 2) {
            System.err.println("Usage: KMeansDriver <input path> <output path>");
            System.exit(-1);
        }

        // 配置Hadoop作业
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Clustering");

        // 设置Jar文件
        job.setJarByClass(task4_part2.class);

        // 设置Mapper、Reducer、Combiner类
        job.setMapperClass(task4_part2_KMeansMapper_Iterative.class);
        job.setCombinerClass(task4_part2_KMeansCombiner.class); // 如果需要Combiner
        job.setReducerClass(task4_part2_KMeansReducer.class);

        // 设置Mapper输出的键值类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置最终Reducer输出的键值类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

        // 提交作业并等待执行结果
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
