package com.hdp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class task4_part2_KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] sum = new double[3]; // 3D空间：dayAvgBytesSent, nightAvgBytesSent, totalAvgBytesSent
        int count = 0;

        // 聚合数据点
        for (Text val : values) {
            String[] fields = val.toString().split(",");
            double dayAvgBytesSent = Double.parseDouble(fields[1]);
            double nightAvgBytesSent = Double.parseDouble(fields[2]);
            double totalAvgBytesSent = Double.parseDouble(fields[3]);
            int frequency = Integer.parseInt(fields[4]);

            sum[0] += dayAvgBytesSent * frequency;
            sum[1] += nightAvgBytesSent * frequency;
            sum[2] += totalAvgBytesSent * frequency;
            count += frequency;
        }

        // 计算新的聚类中心
        double[] newCenter = new double[3];
        newCenter[0] = sum[0] / count;
        newCenter[1] = sum[1] / count;
        newCenter[2] = sum[2] / count;

        // 输出新的聚类中心
        context.write(key, new Text(newCenter[0] + "," + newCenter[1] + "," + newCenter[2] + "," + count));
    }
}
