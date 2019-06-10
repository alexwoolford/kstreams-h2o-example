package io.woolford.kstreams.h2o.example;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        IrisClassifier irisClassifier = new IrisClassifier();
        irisClassifier.run();
    }

}
