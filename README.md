## Introduction

Use [Infinispan](https://github.com/infinispan/infinispan) to manage the joining and leaving of nodes in a [Cajun](https://github.com/CajunSystems/cajun) cluster, in order to manage the target nodes required when the Messaging System sends messages.

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
