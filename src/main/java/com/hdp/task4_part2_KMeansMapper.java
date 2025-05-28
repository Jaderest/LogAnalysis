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
        centers.add(new ClusterCenter(0, new double[] { 10.0, 500.0, 8.0, 400.0 }));
        centers.add(new ClusterCenter(1, new double[] { 20.0, 1500.0, 30.0, 1300.0 }));
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        try {
            String ip = fields[0];

            // 新的特征向量提取
            double dayRequests = extractDouble(fields[3]);
            double dayAvgBytesSent = extractDouble(fields[4]);
            double nightRequests = extractDouble(fields[5]);
            double nightAvgBytesSent = extractDouble(fields[6]);

            double[] point = { dayRequests, dayAvgBytesSent, nightRequests, nightAvgBytesSent };

            // 计算距离并选择最近的中心
            int closestClusterId = -1;
            double minDistance = Double.MAX_VALUE;
            for (ClusterCenter center : centers) {
                double distance = computeEuclideanDistance(point, center.getCenter());
                if (distance < minDistance) {
                    minDistance = distance;
                    closestClusterId = center.getClusterId();
                }
            }

            // 输出格式：IP,4个值,1
            context.write(new IntWritable(closestClusterId),
                    new Text(ip + "," + dayRequests + "," + dayAvgBytesSent + "," + nightRequests + ","
                            + nightAvgBytesSent + ",1"));

        } catch (Exception e) {
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