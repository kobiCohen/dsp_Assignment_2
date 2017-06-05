import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;


public class stepTwoDriver {

    // TODO: 6/5/17 need to delete temp
    int N = 7;
    int totalNumberOfWord = 89;
    static String maxFd = "/home/noam/hadoop-2.8.0/out/max_fd/part-r-00000";
    static String wordCount = "/home/noam/hadoop-2.8.0/out/word_count/part-r-00000";

    public static class TokenizerMapper
            extends Mapper<stepTwoK1, stepTwoV1, stepTwoK1, stepTwoV1> {

        public void map(stepTwoK1 key, stepTwoV1 value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class IntSumReducer
            extends Reducer<stepTwoK1, stepTwoV1, Text, IntWritable> {

        public void reduce(stepTwoK1 key, Iterable<stepTwoV1> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (stepTwoV1 val : values) {
                Text a1 = new Text();
                a1.set("33");
                IntWritable a2 = new IntWritable();
                a2.set(343);
                context.write(a1, a2);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJarByClass(stepWordCountDriver.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
//        job.setMapOutputKeyClass(stepTwoK1.class);
//        job.setMapOutputValueClass(stepTwoV1.class);
        job.setInputFormatClass(stepTwoFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}