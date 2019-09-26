package io.github.sruby.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class PartitionerApp {


    /**
     * 定义 Driver ： 封装了MapReduce作业的所有信息
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        //懒得在IDEA配置 args 参数
        args = new String[2];
        args[0] = "hdfs://127.0.0.1:8020/springhdfs/fruits.txt";
        args[1] = "hdfs://127.0.0.1:8020/output/fruits";

        // 创建 Configuration
        Configuration configuration = new Configuration();

        // 清除已存在的文件目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("outputPath: " + args[1] + " exists, but has been deleted.");
        }

        // 创建 Job
        Job job = Job.getInstance(configuration, "wordcount");

        // 设置Job 的处理类
        job.setJarByClass(PartitionerApp.class);


        // 设置作业处理的输入路径, 通过参数获得
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置map 相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置reduce 相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        // 设置job的Partitioner
        job.setPartitionerClass(MyPartitioner.class);
        // 设置4个reducer， 每个类别一个
        job.setNumReduceTasks(4);


        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

    /**
     * Map： 读取输入的文件
     * LongWritable 偏移量
     * Text 一行一行的文本数据
     * Text 字符串 + 数字 如 home 1
     * LongWritable {1,1,1,1,1}
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 接收到的每一行数据
            String line = value.toString();
            //按照指定分割符号进行拆分
            String[] words = line.split(" ");

            context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));
        }

    }

    /**
     * Reduce: 归并操作
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable value : values) {
                // 求key 出现的次数总和
                sum += value.get();
            }
            // 最终统计结果的输出
            context.write(key, new LongWritable(sum));
        }
    }

    public static class MyPartitioner extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {

            if (key.toString().equals("Apple")) {
                return 0;
            }
            if (key.toString().equals("Orange")) {
                return 1;
            }
            if (key.toString().equals("Pear")) {
                return 2;
            }

            return 3;
        }
    }

}