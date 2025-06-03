package com.hdp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class task4_part2_KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double[] sum = new double[4];
        int count = 0;

        for (Text val : values) {
            String[] fields = val.toString().split(",");
            double dayReq = Double.parseDouble(fields[0]);
            double dayAvg = Double.parseDouble(fields[1]);
            double nightReq = Double.parseDouble(fields[2]);
            double nightAvg = Double.parseDouble(fields[3]);
            int freq = Integer.parseInt(fields[4]);

            sum[0] += dayReq * freq;
            sum[1] += dayAvg * freq;
            sum[2] += nightReq * freq;
            sum[3] += nightAvg * freq;
            count += freq;
        }

        double[] newCenter = new double[4];
        for (int i = 0; i < 4; i++)
            newCenter[i] = sum[i] / count;

        context.write(key,
                new Text(newCenter[0] + ", " + newCenter[1] + ", " + newCenter[2] + ", " + newCenter[3] + ", " + count));
    }

}
