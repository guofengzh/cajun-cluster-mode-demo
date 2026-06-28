package dev.cajun.actor.demo;

public class Hello {
  public static void main(String[] args) {

    final int numberOfArgs = args.length;

    switch (numberOfArgs) {
      case 0 -> System.out.print("Hello World!\n");
      case 1 -> System.out.printf("Hello %s!\n", args[0]);
      default ->
          throw new IllegalArgumentException(
              "Too many arguments provided! I can only greet one person at a time.");
    }
  }
}
