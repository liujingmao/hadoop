package com.lagou.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**git
 * Created by liujingmao on 2020-07-11.
 */
public class HomeWork {

    public static class Map extends Mapper<Object, Text, IntWritable,IntWritable>{

        private static IntWritable data = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data,new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private static IntWritable lineNum = new IntWritable(1);

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values){
                context.write(lineNum, key);
                lineNum = new IntWritable(lineNum.get()+1);
            }
        }
    }

    public static class Partition extends Partitioner<IntWritable, IntWritable> {

        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int maxNumber = 65223;
            int bound = maxNumber / numPartitions + 1;
            int keyNumber = key.get();
            for (int i = 0;i < numPartitions; i++) {
                if (keyNumber > bound*numPartitions && keyNumber < bound *(numPartitions + 1)) {
                    return numPartitions;
                }
            }
            return -1;
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.out.println("Usage: Sort <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(configuration,"HomeWork");
        job.setJarByClass(HomeWork.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job,new Path(otherArgs[1]));
        FileInputFormat.addInputPath(job,new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        System.exit(job.waitForCompletion(true) ? 0: 1);

    }
}