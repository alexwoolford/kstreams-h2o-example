package io.woolford.kstreams.h2o.example;

public class IrisClassifiedRecord {

    private String species;
    private String predictedSpecies;
    private boolean match;

    public String getSpecies() {
        return species;
    }

    public void setSpecies(String species) {
        this.species = species;
        updateMatch();
    }

    public String getPredictedSpecies() {
        return predictedSpecies;
    }

    public void setPredictedSpecies(String predictedSpecies) {
        this.predictedSpecies = predictedSpecies;
        updateMatch();
    }

    public boolean isMatch() {
        return match;
    }

    public void setMatch(boolean match) {
        this.match = match;
    }

    private void updateMatch(){
        if (this.species.equals(this.predictedSpecies)){
            this.match = true;
        } else {
            this.match = false;
        }
    }

    @Override
    public String toString() {
        return "IrisClassifiedRecord{" +
                "species='" + species + '\'' +
                ", predictedSpecies='" + predictedSpecies + '\'' +
                ", match=" + match +
                '}';
    }

}
