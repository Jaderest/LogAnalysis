package com.hdp;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.util.*;

public class task4_part2_KMeansMapper_Iterative extends Mapper<Object, Text, IntWritable, Text>{

    private static List<ClusterCenter> centers = new ArrayList<>();
    private static int initializedCount = 0;
    private static boolean centersInitialized = false;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        // 在Mapper的setup方法中初始化中心点
        if (!centersInitialized) {
            /* Iterative 1 */
            // double[][] rawCenters = {
            //     {1184.6536082474227, 14635.479748811167, 998.420618556701, 14885.085936537078}, // Cluster 0
            //     {19018.682352941178, 15025.328681112289, 16095.058823529413, 14899.135570519837}, // Cluster 1
            //     {3205.762962962963, 16187.043126763569, 2705.259259259259, 16235.595289038505}, // Cluster 2
            //     {542.8436578171091, 14944.997619177739, 458.99705014749264, 15203.534616758085}  // Cluster 3
            // };
            /* Iterative 2 */
            // double[][] rawCenters = {
            //     {622.5684454756381, 14517.98226638229, 524.092807424594, 14835.778174178318}, // Cluster 0 431
            //     {26518.87037037037, 15043.254099718612, 22447.62962962963, 14912.34209920451}, // Cluster 1 54
            //     {3841.885931558935, 15618.45182980707, 3242.3688212927755, 15631.902922169367}, // Cluster 2 263
            //     {328.3277027027027, 15032.860546106309, 278.125, 15273.038739383144}  // Cluster 3 296
            // };
            double[][] rawCenters = {
                {782.6880466472303,  14576.964821865968, 658.5539358600583, 14348.873008709628}, // Cluster 0
                {27673.843137254902, 15041.44019824921,	 23420.49019607843, 14926.243314876174}, // Cluster 1
                {4113.708,	         15668.456343177395, 3472.856,	        15332.652403502554}, // Cluster 2
                {249.23,	         14857.09790319304,	 211.4525,	        15788.579624735317}  // Cluster 3
            };
            double[] point;
            for (int i = 0; i < rawCenters.length; i++) {
                point = new double[] {
                    rawCenters[i][0] / 1000.0,   // dayReq
                    rawCenters[i][1] / 15000.0,  // dayAvgBytesSent
                    rawCenters[i][2] / 1000.0,   // nightReq
                    rawCenters[i][3] / 15000.0   // nightAvgBytesSent
                };
                centers.add(new ClusterCenter(i, point));
            }
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length < 7)
            return;

        try {
            String ip = fields[0];

            // 提取原始值
            double dayRequests = extractDouble(fields[3]);
            double dayAvgBytesSent = extractDouble(fields[4]);
            double nightRequests = extractDouble(fields[5]);
            double nightAvgBytesSent = extractDouble(fields[6]);

            // 原始向量
            double[] raw = new double[] { dayRequests, dayAvgBytesSent, nightRequests, nightAvgBytesSent };

            // 简单归一化
            double[] point = new double[] {
                    raw[0] / 1000.0, // dayReq
                    raw[1] / 15000.0, // dayAvgBytesSent
                    raw[2] / 1000.0, // nightReq
                    raw[3] / 15000.0 // nightAvgBytesSent
            };

            // 初始化中心（前两个点）
            if (!centersInitialized && initializedCount < 4) {
                centers.add(new ClusterCenter(initializedCount, point.clone()));
                initializedCount++;
                if (initializedCount == 4) {
                    centersInitialized = true;
                }
            }

            if (!centersInitialized)
                return; // 等待中心初始化完成

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

        public int getClusterId() {
            return clusterId;
        }

        public double[] getCenter() {
            return center;
        }
    }
}
