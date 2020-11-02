package io.woolford.kstreams.h2o.example;

public class IrisClassifiedRecord {

    private String species;
    private String predictedSpecies;

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
        return "IrisSpeciesPredictedSpeciesRecord{" +
                "species='" + species + '\'' +
                ", predictedSpecies='" + predictedSpecies + '\'' +
                '}';
    }

}
