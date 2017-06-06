import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class stepTwoK1 implements WritableComparable{

    private Text created_at;
    private LongWritable id;

    public Text getRawJson() {
        return rawJson;
    }

    private Text rawJson;

    public stepTwoK1(String json){
        this.created_at = new Text();
        this.rawJson = new Text();
        this.id = new LongWritable();
        tweetParser jsonTweet = new tweetParser(json);
        created_at.set(jsonTweet.getCreatedAt());
        id.set(jsonTweet.getId());
        rawJson.set(json);
    }

    public stepTwoK1(){
        this.created_at = new Text();
        this.id = new LongWritable();
    }

    @Override
    public String toString() {
        return Long.toString(this.id.get());
    }


    @Override
    public int compareTo(Object o) {
//        return id.compareTo(((stepTwoK1)o).getId());
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        created_at.write(out);
        id.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        created_at.readFields(in);
        id.readFields(in);
    }

    public LongWritable getId() {
        return id;
    }

}