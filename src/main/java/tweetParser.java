import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

public class tweetParser {

    private String createdAt;
    private Long id;
    private String userName;
    private String text;
    private Boolean favorited;
    private Boolean retweeted;

    public tweetParser(String tweetJson){
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(tweetJson);
            JSONObject jsonObject = (JSONObject) obj;
            this.createdAt = (String) jsonObject.get("created_at");
            this.id = (Long) jsonObject.get("id");
            this.text = (String) jsonObject.get("text");
            this.favorited = (Boolean) jsonObject.get("favorited");
            this.retweeted = (Boolean) jsonObject.get("retweeted");
            JSONObject userJsonObject = (JSONObject) jsonObject.get("user");
            this.userName = (String) userJsonObject.get("name");
        }
        catch (ParseException pe){
            System.out.println("can't parse the json");
            System.out.println(pe);
        }

    }

    public String getCreatedAt() {
        return createdAt;
    }

    public Long getId() {
        return id;
    }

    public String getUserName() {
        return userName;
    }

    public String getText() {
        return text;
    }

    public Boolean getFavorited() {
        return favorited;
    }

    public Boolean getRetweeted() {
        return retweeted;
    }


    public static void main(String[] args) {
        String json = "{\"created_at\":\"Thu Jan 08 08:03:04 +0000 2015\",\"id\":553099554569322496,\"id_str\":\"553099554569322496\",\"text\":\"HIGH DEAL &gt;&gt; http:\\/\\/t.co\\/Rtba586BMk #52801 Health Care Logistics ErgoMates Anti-fatigue Shoes -1 Each - Large\\n\\n... http:\\/\\/t.co\\/AlJPbkLkTp\",\"source\":\"\\u003ca href=\\\"http:\\/\\/ifttt.com\\\" rel=\\\"nofollow\\\"\\u003eIFTTT\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":2713235258,\"id_str\":\"2713235258\",\"name\":\"Run Deals \",\"screen_name\":\"Rundeals_782\",\"location\":\"USA\",\"url\":null,\"description\":null,\"protected\":false,\"verified\":false,\"followers_count\":88,\"friends_count\":4,\"listed_count\":4,\"favourites_count\":6,\"statuses_count\":112282,\"created_at\":\"Wed Aug 06 23:58:25 +0000 2014\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":false,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C0DEED\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"0084B4\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/497171035188260865\\/HaxT0Kmw_normal.jpeg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/497171035188260865\\/HaxT0Kmw_normal.jpeg\",\"default_profile\":true,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"trends\":[],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/Rtba586BMk\",\"expanded_url\":\"http:\\/\\/ift.tt\\/1GYUOSD\",\"display_url\":\"ift.tt\\/1GYUOSD\",\"indices\":[19,41]}],\"user_mentions\":[],\"symbols\":[],\"media\":[{\"id\":553099554531573760,\"id_str\":\"553099554531573760\",\"indices\":[121,143],\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/B60BHUlIAAAUQ72.jpg\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/B60BHUlIAAAUQ72.jpg\",\"url\":\"http:\\/\\/t.co\\/AlJPbkLkTp\",\"display_url\":\"pic.twitter.com\\/AlJPbkLkTp\",\"expanded_url\":\"http:\\/\\/twitter.com\\/Rundeals_782\\/status\\/553099554569322496\\/photo\\/1\",\"type\":\"photo\",\"sizes\":{\"medium\":{\"w\":140,\"h\":140,\"resize\":\"fit\"},\"small\":{\"w\":140,\"h\":140,\"resize\":\"fit\"},\"thumb\":{\"w\":140,\"h\":140,\"resize\":\"crop\"},\"large\":{\"w\":140,\"h\":140,\"resize\":\"fit\"}}}]},\"extended_entities\":{\"media\":[{\"id\":553099554531573760,\"id_str\":\"553099554531573760\",\"indices\":[121,143],\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/B60BHUlIAAAUQ72.jpg\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/B60BHUlIAAAUQ72.jpg\",\"url\":\"http:\\/\\/t.co\\/AlJPbkLkTp\",\"display_url\":\"pic.twitter.com\\/AlJPbkLkTp\",\"expanded_url\":\"http:\\/\\/twitter.com\\/Rundeals_782\\/status\\/553099554569322496\\/photo\\/1\",\"type\":\"photo\",\"sizes\":{\"medium\":{\"w\":140,\"h\":140,\"resize\":\"fit\"},\"small\":{\"w\":140,\"h\":140,\"resize\":\"fit\"},\"thumb\":{\"w\":140,\"h\":140,\"resize\":\"crop\"},\"large\":{\"w\":140,\"h\":140,\"resize\":\"fit\"}}}]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"medium\",\"lang\":\"en\",\"timestamp_ms\":\"1420704184559\"}";
        tweetParser tw = new tweetParser(json);
    }
}
