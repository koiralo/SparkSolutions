package com.shankar.generics.example1;

/**
 * Created by sakoirala on 6/12/17.
 */
public class MainClass {
    public static void main(String[] args) {

        Glass <Juice> Jglass = new Glass();
        Glass <Water> Wglass = new Glass();
        Juice juice = new Juice();
        Water water = new Water();

        Jglass.liquid = juice;
        Wglass.liquid = water;

        Juice j = Jglass.liquid;

        Water w = Wglass.liquid;

    }

}
