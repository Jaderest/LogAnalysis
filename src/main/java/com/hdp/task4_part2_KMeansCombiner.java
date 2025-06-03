package com.hdp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class task4_part2_KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double[] sum = new double[4];
        int count = 0;

        for (Text val : values) {
            // 读取Mapper传来的数据，求均值
            String[] fields = val.toString().split(",");
            double dayReq = Double.parseDouble(fields[1]);
            double dayAvg = Double.parseDouble(fields[2]);
            double nightReq = Double.parseDouble(fields[3]);
            double nightAvg = Double.parseDouble(fields[4]);
            int freq = Integer.parseInt(fields[5]);

            sum[0] += dayReq * freq;
            sum[1] += dayAvg * freq;
            sum[2] += nightReq * freq;
            sum[3] += nightAvg * freq;
            count += freq;
        }

        // 计算新的中心点
        double[] newCenter = new double[4];
        for (int i = 0; i < 4; i++)
            newCenter[i] = sum[i] / count;

        context.write(key, new Text(String.join(",",
                String.valueOf(newCenter[0]),
                String.valueOf(newCenter[1]),
                String.valueOf(newCenter[2]),
                String.valueOf(newCenter[3]),
                String.valueOf(count))));
    }

}
