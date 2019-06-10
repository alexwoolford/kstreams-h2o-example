package io.woolford.kstreams.h2o.example;

public class IrisRecord {

    private double sepalLength;
    private double sepalWidth;
    private double petalLength;
    private double petalWidth;
    private String species;
    private String predictedSpecies;

    public double getSepalLength() {
        return sepalLength;
    }

    public void setSepalLength(double sepalLength) {
        this.sepalLength = sepalLength;
    }

    public double getSepalWidth() {
        return sepalWidth;
    }

    public void setSepalWidth(double sepalWidth) {
        this.sepalWidth = sepalWidth;
    }

    public double getPetalLength() {
        return petalLength;
    }

    public void setPetalLength(double petalLength) {
        this.petalLength = petalLength;
    }

    public double getPetalWidth() {
        return petalWidth;
    }

    public void setPetalWidth(double petalWidth) {
        this.petalWidth = petalWidth;
    }

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    public String getPredictedSpecies() {
        return predictedSpecies;
    }

    public void setPredictedSpecies(String predictedSpecies) {
        this.predictedSpecies = predictedSpecies;
    }

    @Override
    public String toString() {
        return "IrisRecord{" +
                "sepalLength=" + sepalLength +
                ", sepalWidth=" + sepalWidth +
                ", petalLength=" + petalLength +
                ", petalWidth=" + petalWidth +
                ", species='" + species + '\'' +
                ", predictedSpecies='" + predictedSpecies + '\'' +
                '}';
    }

}
