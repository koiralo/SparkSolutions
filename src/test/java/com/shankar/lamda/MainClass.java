package com.shankar.lamda;

/**
 * Created by sakoirala on 6/12/17.
 */
public class MainClass {

    public static void main(String[] args) {

        Greeting greeting = new Greeting() {
            @Override
            public void perform() {
                System.out.println("Hello world from anyon");
            }
        };

        greeting.perform();

        Greeting g = new HelloGreeting();
        g.perform();


//        System.out.println(System.getProperty("java.version"));
        Greeting gLamda = () -> System.out.println("hello world lamda");
        gLamda.perform();


    }
}
