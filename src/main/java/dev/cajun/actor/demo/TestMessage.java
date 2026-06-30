package dev.cajun.actor.demo;

public class TestMessage implements java.io.Serializable {
    private final String content;

    public TestMessage(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}