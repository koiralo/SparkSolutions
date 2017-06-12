package com.shankar.lamda;

/**
 * Created by sakoirala on 6/12/17.
 */
public class HelloGreeting implements Greeting {
    @Override
    public void perform() {
        System.out.println("Hello world");
    }
}
