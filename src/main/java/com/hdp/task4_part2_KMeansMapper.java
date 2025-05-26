package com.hdp;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.util.ArrayList;
import java.util.List;
import java.io.*;

public class task4_part2_KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

    private static List<ClusterCenter> centers = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        centers.add(new ClusterCenter(0, new double[] { 1.0, 1.0 }));
        centers.add(new ClusterCenter(1, new double[] { 5.0, 5.0 }));
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        try {
            String ip = fields[0];

            // 提取冒号后的数值并转成 double
            double dayAvgBytesSent = extractDouble(fields[4]);
            double nightAvgBytesSent = extractDouble(fields[6]);
            double totalAvgBytesSent = extractDouble(fields[7]);

            double[] point = { dayAvgBytesSent, nightAvgBytesSent };

            int closestClusterId = -1;
            double minDistance = Double.MAX_VALUE;

            for (ClusterCenter center : centers) {
                double distance = computeEuclideanDistance(point, center.getCenter());
                if (distance < minDistance) {
                    minDistance = distance;
                    closestClusterId = center.getClusterId();
                }
            }

            context.write(new IntWritable(closestClusterId),
                    new Text(ip + "," + dayAvgBytesSent + "," + nightAvgBytesSent + "," + totalAvgBytesSent + ",1"));

        } catch (Exception e) {
            // 打印出错行内容方便调试
            System.err.println("Skipping line due to error: " + value.toString());
            e.printStackTrace();
        }
    }

    private double extractDouble(String field) {
        String[] parts = field.split(":");
        return Double.parseDouble(parts.length > 1 ? parts[1] : parts[0]);
    }

    private double computeEuclideanDistance(double[] point, double[] center) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            sum += Math.pow(point[i] - center[i], 2);
        }
        return Math.sqrt(sum);
    }

    // 聚类中心类
    public static class ClusterCenter {
        private int clusterId;
        private double[] center;

        public ClusterCenter(int clusterId, double[] center) {
            this.clusterId = clusterId;
            this.center = center;
        }

        public int getClusterId() {
            return clusterId;
        }

        public double[] getCenter() {
            return center;
        }
    }
}