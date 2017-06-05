import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class stepTwoV1 implements WritableComparable{

    private Text userName;
    private Text text;
    private BooleanWritable favorited;
    private BooleanWritable retweeted;

    public stepTwoV1(String json){
        this.userName = new Text();
        this.text = new Text();
        this.favorited = new BooleanWritable();
        this.retweeted = new BooleanWritable();
        tweetParser jsonTweet = new tweetParser(json);
        userName.set(jsonTweet.getUserName());
        text.set(jsonTweet.getText());
        favorited.set(jsonTweet.getFavorited());
        retweeted.set(jsonTweet.getRetweeted());
    }

    public stepTwoV1(){
        this.userName = new Text();
        this.text = new Text();
        this.favorited = new BooleanWritable();
        this.retweeted = new BooleanWritable();
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        userName.write(out);
        text.write(out);
        favorited.write(out);
        retweeted.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userName.readFields(in);
        text.readFields(in);
        favorited.readFields(in);
        retweeted.readFields(in);
    }

    public Text getUserName() {
        return userName;
    }

    public Text getText() {
        return text;
    }

    public BooleanWritable getFavorited() {
        return favorited;
    }

    public BooleanWritable getRetweeted() {
        return retweeted;
    }


}