
public class lineParser {

    public String getWord() {
        return word;
    }

    private String word;

    public long getNum() {
        return num;
    }

    private long num;

    public  lineParser(String line){
        String[] splitString = line.split("\\s+");
        this.word = splitString[0];
        this.num = Long.parseLong(splitString[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof lineParser)) return false;

        lineParser that = (lineParser) o;

        return word != null ? word.equals(that.word) : that.word == null;
    }

}
