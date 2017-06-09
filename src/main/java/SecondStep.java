
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class SecondStep
     class SecondMap extends Mapper<Text,Text,Text,Text>{
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            tweetParser tweet = new tweetParser(key);
           String TweetText = tweet.getText().replaceAll("," , "").replaceAll(".", "");
           String [] tweetWords = TweetText.split(" ");

            Arrays.stream(tweetWords).
                    filter(s -> StopWords.IsStopWord(s)).
                    forEach(w -> {
                      Text text = new Text(w);
                        try {
                            context.write(text,key );
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });

        }

    }
     class SecondReduce extends Reducer<Text, Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Text> Tweets = new ArrayList<>();
            values.forEach(v -> Tweets.add(v));

            Text outputList = new Text(Tweets.toString());
            context.write(key, outputList);

        }

     }
    }
}