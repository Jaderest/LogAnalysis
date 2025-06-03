package com.hdp;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.util.*;

public class task4_part2_KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

    private static List<ClusterCenter> centers = new ArrayList<>();
    private static int initializedCount = 0;
    private static boolean centersInitialized = false;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length < 7) return;

        try {
            String ip = fields[0];

            // 提取原始值
            double dayRequests = extractDouble(fields[3]);
            double dayAvgBytesSent = extractDouble(fields[4]);
            double nightRequests = extractDouble(fields[5]);
            double nightAvgBytesSent = extractDouble(fields[6]);

            // 原始向量
            double[] raw = new double[] { dayRequests, dayAvgBytesSent, nightRequests, nightAvgBytesSent };

            // 简单归一化（除以经验最大值）
            double[] point = new double[] {
                raw[0] / 1000.0,   // dayReq
                raw[1] / 20000.0,  // dayAvgBytesSent
                raw[2] / 1000.0,   // nightReq
                raw[3] / 20000.0   // nightAvgBytesSent
            };

            // 初始化中心（前两个点）
            if (!centersInitialized && initializedCount < 3) {
                centers.add(new ClusterCenter(initializedCount, point.clone()));
                initializedCount++;
                if (initializedCount == 3) {
                    centersInitialized = true;
                }
            }

            if (!centersInitialized) return; // 等待中心初始化完成

            // 距离比较
            int closestClusterId = -1;
            double minDistance = Double.MAX_VALUE;
            for (ClusterCenter center : centers) {
                double distance = computeEuclideanDistance(point, center.getCenter());
                if (distance < minDistance) {
                    minDistance = distance;
                    closestClusterId = center.getClusterId();
                }
            }

            // 输出格式：原始值+1
            context.write(new IntWritable(closestClusterId),
                new Text(ip + "," + raw[0] + "," + raw[1] + "," + raw[2] + "," + raw[3] + ",1"));

        } catch (Exception e) {
            System.err.println("Skipping line: " + value.toString());
        }
    }

    private double extractDouble(String field) {
        String[] parts = field.split(":");
        return Double.parseDouble(parts.length > 1 ? parts[1] : parts[0]);
    }

    private double computeEuclideanDistance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(sum);
    }

    public static class ClusterCenter {
        private int clusterId;
        private double[] center;

        public ClusterCenter(int id, double[] c) {
            this.clusterId = id;
            this.center = c;
        }

        public int getClusterId() { return clusterId; }
        public double[] getCenter() { return center; }
    }
}
