package eu.nyuu.courses.model;

public class FeelingEvent extends TweetEvent {
    private String feeling;

    public FeelingEvent(TweetEvent tweetEvent, String feeling) {
        super();
        this.feeling = feeling;
    }

    public String getFeeling() {
        return feeling;
    }

    public void setFeeling(String feeling) {
        this.feeling = feeling;
    }
}
