package io.woolford.kafkairisdata;

public class IrisRecord {

    private Double sepalLength;
    private Double sepalWidth;
    private Double petalLength;
    private Double petalWidth;
    private String species;

    public Double getSepalLength() {
        return sepalLength;
    }

    public void setSepalLength(Double sepalLength) {
        this.sepalLength = sepalLength;
    }

    public Double getSepalWidth() {
        return sepalWidth;
    }

    public void setSepalWidth(Double sepalWidth) {
        this.sepalWidth = sepalWidth;
    }

    public Double getPetalLength() {
        return petalLength;
    }

    public void setPetalLength(Double petalLength) {
        this.petalLength = petalLength;
    }

    public Double getPetalWidth() {
        return petalWidth;
    }

    public void setPetalWidth(Double petalWidth) {
        this.petalWidth = petalWidth;
    }

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    @Override
    public String toString() {
        return "IrisRecord{" +
                "sepalLength=" + sepalLength +
                ", sepalWidth=" + sepalWidth +
                ", petalLength=" + petalLength +
                ", petalWidth=" + petalWidth +
                ", species='" + species + '\'' +
                '}';
    }

}
