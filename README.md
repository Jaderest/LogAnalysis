# README

这是南京大学2025春大数据实验 课程设计2 的代码实现。

## 编译方式
```bash
mvn clean package
# 在target中可以找到jar包
```

## 运行方式
```bash
hadoop jar <jar path> com.hdp.task<1|2> <input> <output>

hadoop jar <jar path> com.hdp.task3_part1 <input> <output>
hadoop jar <jar path> com.hdp.task3_part2 <input> <output>
# 此处agent_sort_file为task2的输出文件中以_agent_sorted为后缀的目录中的part文本文件
# 例如：<hadoop|yarn> jar /home/gr58/log_analysis-1.0.jar com.hdp.task3_part3 /user/root/FinalExp/FinalExp2/dataset/ /user/gr58/do_for_log_screenshoot/task3_3 /user/gr58/final/output/task2_agent_sorted/part-r-00000
hadoop jar <jar path> com.hdp.task3_part3 <input> <output> <agent_sort_file>

hadoop jar <jar path> com.hdp.task4_part1 <input> <output>
# 此处input为task4_part1的输出路径
hadoop jar <jar path> com.hdp.task4_part2 <input> <output>
# 运行task4_part2迭代求新cluster中心，input为原始数据集
hadoop jar <jar path> com.hdp.task4_part2_Iterative <input> <output>
```

## 小组成员
[江思源 221840206, 廖琪 221220105]