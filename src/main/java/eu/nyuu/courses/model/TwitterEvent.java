package eu.nyuu.courses.model;

public class TwitterEvent {
    protected String id;
    protected String timestamp;
    protected String nick;
    protected String body;

    public TwitterEvent() {
        this.id = "";
        this.timestamp = "";
        this.nick = "";
        this.body = "";
    }

    public TwitterEvent(TwitterEvent copy) {
        this.id = copy.id;
        this.timestamp = copy.timestamp;
        this.nick = copy.nick;
        this.body = copy.body;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getNick() {
        return nick;
    }

    public void setNick(String nick) {
        this.nick = nick;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
