import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class stepMaxfdRecordReader extends
        RecordReader<LongWritable, Text> {

    LineRecordReader lineReader;
    Text value;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        lineReader = new LineRecordReader();
        lineReader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!lineReader.nextKeyValue())
            return false;
        String line = lineReader.getCurrentValue().toString();
        tweetParser jsonTweetParser = new tweetParser(line);
        value = new Text();
        value.set(jsonTweetParser.getText());
        return true;
    }

    @Override
    public LongWritable getCurrentKey(){
        return lineReader.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return lineReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }
}