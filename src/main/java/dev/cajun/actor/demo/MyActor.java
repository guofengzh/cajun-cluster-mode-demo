package dev.cajun.actor.demo;

import com.cajunsystems.Actor;
import com.cajunsystems.ActorSystem;

public class MyActor extends Actor<TestMessage> {
    public MyActor(ActorSystem system, String actorId) {
        super(system, actorId);
    }

    @Override
    protected void receive(TestMessage message) {
        System.out.println("[DEBUG_LOG] Child " + getActorId() + " received: " + message.getContent());
    }
}
