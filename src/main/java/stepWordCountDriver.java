import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class stepWordCountDriver {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {


        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] stringWordAarry = value.toString().split("\\s+");
            Word[] wordArray = new Word[stringWordAarry.length];
            for (int i = 0; i < stringWordAarry.length; i++) {
                wordArray[i] = new Word(stringWordAarry[i]);
            }

            for (int i = 0; i < stringWordAarry.length; i++) {
                if (wordArray[i] != null) {
                    word.set(wordArray[i].getWord());
                    String newWord = wordArray[i].getWord();
                    context.write(word, one);
                    for (int j = 0; j < stringWordAarry.length; j++) {
                        if (wordArray[j] != null && wordArray[j].getWord().equals(newWord)) {
                            wordArray[j] = null;
                        }
                    }
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(stepWordCountDriver.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setInputFormatClass(stepWordCountFileInputFormat.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}