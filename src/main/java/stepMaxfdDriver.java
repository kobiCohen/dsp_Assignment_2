import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class stepMaxfdDriver {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable wordCount = new IntWritable(0);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            the size of the array all cells above are null
            int arraySize = 140;
            StringTokenizer itr = new StringTokenizer(value.toString());
//            the array size is blocked by 140 because the tweet is blocked
            Word [] wordArray = new Word[140];
//            get all the token into an array exit the for if there are no token
            for (int i = 0; i < 140; i++){
                if(itr.hasMoreTokens()){
                    wordArray[i] = new Word(itr.nextToken());
                }else{
                    arraySize = i;
                    break;
                }
            }

//            run over the every word in the array
            for (int i = 0; i < arraySize; i++) {
                Word newWord = wordArray[i];
                String wordString = newWord.getWord();

//                enter here only if the word is not equal to "" and it not a stop word
                if (!newWord.getWord().equals("") && !newWord.isStopWord()){
                    int counter = 0;
//                    count the number word in the array
                    for (int j = 0; j < arraySize; j++) {
                        String word = wordArray[j].getWord();
                        if (wordString.equals(word)){
                            counter++;
                            wordArray[j].setWord("");
                        }
                    }
                    wordCount.set(counter);
                    word.set(wordString);
                    context.write(word, wordCount);
                }
            }



        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int max = 0;
            for (IntWritable val : values) {
                if(max < val.get()){
                    max = val.get();
                }
            }
            result.set(max);
            context.write(key, result);
        }
    }





    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step Max fd Driver");
        job.setJarByClass(stepMaxfdDriver.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setInputFormatClass(stepMaxfdFileInputFormat.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}