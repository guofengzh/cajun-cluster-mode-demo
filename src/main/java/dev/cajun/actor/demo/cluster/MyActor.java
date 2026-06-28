package dev.cajun.actor.demo.cluster;

import com.cajunsystems.Actor;
import com.cajunsystems.ActorSystem;
import com.cajunsystems.config.BackpressureConfig;
import com.cajunsystems.config.MailboxConfig;

public class MyActor extends Actor {
    protected MyActor(ActorSystem system, String actorId, BackpressureConfig backpressureConfig, MailboxConfig mailboxConfig) {
        super(system, actorId, backpressureConfig, mailboxConfig);
    }

    @Override
    protected void receive(Object o) {

    }
}
