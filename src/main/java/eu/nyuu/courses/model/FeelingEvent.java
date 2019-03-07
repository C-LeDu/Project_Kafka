package eu.nyuu.courses.model;

public class FeelingEvent extends TwitterEvent {
    private String feeling;

    public FeelingEvent(TwitterEvent twitterEvent, String feeling) {
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
