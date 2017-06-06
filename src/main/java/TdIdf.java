import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TdIdf {

    private String maxFdLoc;
    private String wordCount;
    private int totalTweet;
    private  Word[] tweetArray;
    private MapWritable tdIdfDic;

    public TdIdf(String maxFdLoc, String wordCount, Word[] tweetArray, int totalTweet) throws IOException {
        this.maxFdLoc = maxFdLoc;
        this.wordCount = wordCount;
        this.tweetArray = tweetArray;
        this.totalTweet = totalTweet;
        this.tdIdfDic = new MapWritable();

    }

    public MapWritable start() throws IOException {
        for (Word newWord : tweetArray) {
            if (!newWord.isStopWord()) {
                double temp = tf(newWord);
                double temp1 = idf(newWord);
                double tfidf = temp * temp1;
                DoubleWritable writableTfIdf  = new DoubleWritable();
                writableTfIdf.set(tfidf);
                Text newText = new Text();
                newText.set(newWord.getWord());
                this.tdIdfDic.put(newText, writableTfIdf);
            }
        }
        return tdIdfDic;
    }

    private double tf(Word newWord) throws IOException {
        long wordCounter = 0;
        for (Word word : tweetArray) {
            if(word.equals(newWord)){
                wordCounter++;
            }
        }
        long maxFd = 0;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(maxFdLoc), StandardCharsets.UTF_8)) {
            for (String line = null; (line = br.readLine()) != null;) {
                lineParser newLine = new lineParser(line);
                if (newLine.getWord().equals(newWord.getWord())){
                    maxFd = newLine.getNum();
                    break;
                }
            }
        }
        if (maxFd == 0){
            return 0;
        }
        return (0.5 + 0.5 * (wordCounter / maxFd));
    }

    private double idf(Word newWord) throws IOException {
        long totalWordCount = 0;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(wordCount), StandardCharsets.UTF_8)) {
            for (String line = null; (line = br.readLine()) != null; ) {
                lineParser newLine = new lineParser(line);
                if (newLine.getWord().equals(newWord.getWord())) {
                    totalWordCount = newLine.getNum();
                    break;
                }
            }
        }
        return Math.log(this.totalTweet / totalWordCount);
    }

    public static void main(String[] args) throws IOException {
        Word newWord = new Word("noam");
        Word[] array = new Word[1];
        array[0] = newWord;
        String max = "/home/noam/hadoop-2.8.0/out/max_fd/part-r-00000";
        String wc = "/home/noam/hadoop-2.8.0/out/word_count/part-r-00000";
        TdIdf x = new TdIdf(max ,wc ,array, 7);
        MapWritable Lol = x.start();
        System.out.println(Lol.get("noam"));
    }
}
