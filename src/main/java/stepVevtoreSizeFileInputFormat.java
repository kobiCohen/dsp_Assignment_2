import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class stepVevtoreSizeFileInputFormat extends FileInputFormat<LongWritable, Text> {
    public stepVevtoreSizeFileInputFormat() {
    }

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new stepVecotreSizeRecordReader();
    }

}
