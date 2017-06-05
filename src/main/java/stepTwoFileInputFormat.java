import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class stepTwoFileInputFormat extends FileInputFormat<stepTwoK1, stepTwoV1> {
    public stepTwoFileInputFormat() {
    }

    public RecordReader<stepTwoK1, stepTwoV1> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new stepTwoRecordReader();
    }

}
