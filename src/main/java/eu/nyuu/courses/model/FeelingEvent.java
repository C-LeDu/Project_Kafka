package eu.nyuu.courses.model;

public class FeelingEvent extends TwitterEvent {
    private String feeling;

    public FeelingEvent() {
        super();
        feeling = "";
    }

    public FeelingEvent(TwitterEvent twitterEvent, String feeling) {
        super(twitterEvent);
        this.feeling = feeling;
    }

    public String getFeeling() {
        return feeling;
    }

    public void setFeeling(String feeling) {
        this.feeling = feeling;
    }
}
