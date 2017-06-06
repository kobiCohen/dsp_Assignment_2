import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;


public class stepTwoK2 implements WritableComparable{

    private Text userName;
    private Text created_at;
    private LongWritable id;
    private MapWritable vector;
    private BooleanWritable favorited;
    private BooleanWritable retweeted;
    private Text text;

    public double getSquareRoot() {
        return squareRoot;
    }

    private double squareRoot;


    public stepTwoK2(String json, MapWritable vector){
        this.created_at = new Text();
        this.userName = new Text();
        this.text = new Text();
        this.favorited = new BooleanWritable();
        this.retweeted = new BooleanWritable();
        this.id = new LongWritable();
        this.vector = vector;

        tweetParser jsonTweet = new tweetParser(json);
        id.set(jsonTweet.getId());
        created_at.set(jsonTweet.getCreatedAt());
        userName.set(jsonTweet.getUserName());
        text.set(jsonTweet.getText());
        favorited.set(jsonTweet.getFavorited());
        retweeted.set(jsonTweet.getRetweeted());
        this.squareRoot = squareRootCul();
    }

    private double squareRootCul() {
        double counter = 0;
        Iterable<Writable>  valList = this.vector.values();
        for(Writable write :valList){
            DoubleWritable num = (DoubleWritable) write;
            counter = counter + Math.pow(num.get(), 2);
        }
        return counter;
    }

    public stepTwoK2(){
        this.created_at = new Text();
        this.userName = new Text();
        this.text = new Text();
        this.favorited = new BooleanWritable();
        this.retweeted = new BooleanWritable();
        this.id = new LongWritable();
        this.vector = new MapWritable();
    }

    @Override
    public String toString() {
        return "id: "+this.id.get();
    }



    @Override
    public void write(DataOutput out) throws IOException {
        created_at.write(out);
        id.write(out);
        vector.write(out);
        favorited.write(out);
        retweeted.write(out);
        text.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        created_at.readFields(in);
        id.readFields(in);
        vector.readFields(in);
        favorited.readFields(in);
        retweeted.readFields(in);
        text.readFields(in);
    }

    public double cosineSimilarity(Object o){
        stepTwoK2 otherKey = (stepTwoK2) o;
        double product = dotProduct(otherKey);
        return product / (otherKey.getSquareRoot() * this.squareRoot);
    }

    private double dotProduct(stepTwoK2 otherKey) {
        double counter = 0;
        Set<Writable> keyList = this.vector.keySet();
        for(Writable key: keyList){
            Text keyText = (Text) key;
            if(otherKey.getVector().containsKey(keyText)){
                DoubleWritable thisVal = (DoubleWritable) this.vector.get(keyText);
                DoubleWritable otherVal = (DoubleWritable) otherKey.getVector().get(keyText);
                counter = counter + (thisVal.get() * otherVal.get());
            }
        }
        return counter;
    }


    @Override
    public int compareTo(Object o) {
        if (cosineSimilarity(o) != 0){
            return 0;
        }
        return 1;

    }

    public MapWritable getVector() {
        return vector;
    }


    public static void main(String[] args) throws IOException {
        String json  = "{\"created_at\":\"Thu Jan 08 08:03:04 +0000 2015\",\"id\":553099554569322496,\"id_str\":\"553099554569322496\",\"text\":\"noam noam noam HIGH DEAL &gt;&gt; http:\\/\\/t.co\\/Rtba586BMk #52801 Health Care Logistics ErgoMates Anti-fatigue Shoes -1 Each - Large\\n\\n... http:\\/\\/t.co\\/AlJPbkLkTp\",\"source\":\"\\u003ca href=\\\"http:\\/\\/ifttt.com\\\" rel=\\\"nofollow\\\"\\u003eIFTTT\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":2713235258,\"id_str\":\"2713235258\",\"name\":\"Run Deals \",\"screen_name\":\"Rundeals_782\",\"location\":\"USA\",\"url\":null,\"description\":null,\"protected\":false,\"verified\":false,\"followers_count\":88,\"friends_count\":4,\"listed_count\":4,\"favourites_count\":6,\"statuses_count\":112282,\"created_at\":\"Wed Aug 06 23:58:25 +0000 2014\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":false,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C0DEED\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"0084B4\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/497171035188260865\\/HaxT0Kmw_normal.jpeg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/497171035188260865\\/HaxT0Kmw_normal.jpeg\",\"default_profile\":true,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"trends\":[],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/Rtba586BMk\",\"expanded_url\":\"http:\\/\\/ift.tt\\/1GYUOSD\",\"display_url\":\"ift.tt\\/1GYUOSD\",\"indices\":[19,41]}],\"user_mentions\":[],\"symbols\":[],\"media\":[{\"id\":553099554531573760,\"id_str\":\"553099554531573760\",\"indices\":[121,143],\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/B60BHUlIAAAUQ72.jpg\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/B60BHUlIAAAUQ72.jpg\",\"url\":\"http:\\/\\/t.co\\/AlJPbkLkTp\",\"display_url\":\"pic.twitter.com\\/AlJPbkLkTp\",\"expanded_url\":\"http:\\/\\/twitter.com\\/Rundeals_782\\/status\\/553099554569322496\\/photo\\/1\",\"type\":\"photo\",\"sizes\":{\"medium\":{\"w\":140,\"h\":140,\"resize\":\"fit\"},\"small\":{\"w\":140,\"h\":140,\"resize\":\"fit\"},\"thumb\":{\"w\":140,\"h\":140,\"resize\":\"crop\"},\"large\":{\"w\":140,\"h\":140,\"resize\":\"fit\"}}}]},\"extended_entities\":{\"media\":[{\"id\":553099554531573760,\"id_str\":\"553099554531573760\",\"indices\":[121,143],\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/B60BHUlIAAAUQ72.jpg\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/B60BHUlIAAAUQ72.jpg\",\"url\":\"http:\\/\\/t.co\\/AlJPbkLkTp\",\"display_url\":\"pic.twitter.com\\/AlJPbkLkTp\",\"expanded_url\":\"http:\\/\\/twitter.com\\/Rundeals_782\\/status\\/553099554569322496\\/photo\\/1\",\"type\":\"photo\",\"sizes\":{\"medium\":{\"w\":140,\"h\":140,\"resize\":\"fit\"},\"small\":{\"w\":140,\"h\":140,\"resize\":\"fit\"},\"thumb\":{\"w\":140,\"h\":140,\"resize\":\"crop\"},\"large\":{\"w\":140,\"h\":140,\"resize\":\"fit\"}}}]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"medium\",\"lang\":\"en\",\"timestamp_ms\":\"1420704184559\"}";
        Word newWord = new Word("noam");
        Word[] array = new Word[1];
        array[0] = newWord;
        String max = "/home/noam/hadoop-2.8.0/out/max_fd/part-r-00000";
        String wc = "/home/noam/hadoop-2.8.0/out/word_count/part-r-00000";
        TdIdf x = new TdIdf(max ,wc ,array, 2);
        MapWritable Lol = x.start();
        stepTwoK2 tempo = new stepTwoK2(json, Lol);
        stepTwoK2 tempo1 = new stepTwoK2(json, Lol);
        tempo1.compareTo(tempo);
    }
}