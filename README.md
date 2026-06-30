## Introduction

This is a demonstration using a [Cajun](https://github.com/CajunSystems/cajun) cluster. It uses [Infinispan](https://github.com/infinispan/infinispan) to manage the joining and leaving of nodes in the Cajun cluster, thus managing the list of target nodes required when the Cajun messaging system sends messages.

## Usage 

Launch App1:
```shell
mvnw exec:java -Dexec.mainClass="dev.cajun.actor.demo.App1"
```

Then launch App2:
```shell
mvn exec:java -Dexec.mainClass="dev.cajun.actor.demo.App2"
```

App1 will display the message sent by App2.

## What we did

1. We implemented our own MetadataStore using Infinispan.
2. We use Infinispan's cluster listener to learn about changes in network topology and node status.

## Why do we care about Cajun?

The AI agent behaves much like an actor. Cajun is a modern, lightweight agent system (compared to [Apache Pekko](https://github.com/apache/pekko) and Lightbend's [Akka](https://github.com/akka/akka-core)), making it well-suited for developing distributed AI agent systems.

Please see Pradeep Samuel's article "[Building AI Agents with Actors in Java: A Natural Fit](https://medium.com/@pradeepsamd/building-ai-agents-with-actors-in-java-a-natural-fit-3309cd2ea9a9)".
