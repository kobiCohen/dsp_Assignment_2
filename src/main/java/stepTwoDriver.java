import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
    static int N = 100;
    int totalNumberOfWord = 89;
    static String maxFd = "/home/noam/hadoop-2.8.0/out/max_fd/part-r-00000";
    static String wordCount = "/home/noam/hadoop-2.8.0/out/word_count/part-r-00000";

    public static class TokenizerMapper
            extends Mapper<stepTwoK1, stepTwoV1, stepTwoK2, stepTwoK2> {

        public void map(stepTwoK1 key, stepTwoV1 value, Context context) throws IOException, InterruptedException {
            String text = value.getText().toString();

            String[] stringWordArry = text.split("\\s+");
            Word[]  wordArray  = new Word[stringWordArry.length];
            for (int i = 0; i < stringWordArry.length; i++) {
                wordArray[i] = new Word(stringWordArry[i]);
            }


            TdIdf idf = new TdIdf(maxFd, wordCount, wordArray ,N);
            MapWritable map = idf.start();
            stepTwoK2 newKey =  new stepTwoK2(key.getRawJson().toString(), map);
            context.write(newKey, newKey);
        }
    }

    public static class IntSumReducer
            extends Reducer<stepTwoK2, stepTwoK2, Text, LongWritable> {

        public void reduce(stepTwoK2 key, Iterable<stepTwoK2> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (stepTwoK2 val : values) {
                Text a1 = new Text();
                a1.set("33");
                LongWritable a2 = new LongWritable();
                a2.set((long) val.getSquareRoot());
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
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
//        map emit k/v
        job.setMapOutputKeyClass(stepTwoK2.class);
        job.setMapOutputValueClass(stepTwoK2.class);
//        reduce emit k/v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
//        ser input format
        job.setInputFormatClass(stepTwoFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}