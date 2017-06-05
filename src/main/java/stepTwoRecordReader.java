import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class stepTwoRecordReader extends
        RecordReader<stepTwoK1, stepTwoV1> {

    LineRecordReader lineReader;
    stepTwoK1 key;
    stepTwoV1 value;


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
        value = new stepTwoV1(line);
        key = new stepTwoK1(line);
        return true;
    }

    @Override
    public stepTwoK1 getCurrentKey(){
        return key;
    }

    @Override
    public stepTwoV1 getCurrentValue() {
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