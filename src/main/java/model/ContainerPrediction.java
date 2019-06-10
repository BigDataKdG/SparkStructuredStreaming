package model;

import java.util.Objects;

public class ContainerPrediction {

    private Integer containerNummer;
    private Double probability;
    private String predictedLabel;

    public ContainerPrediction(final Integer containerNummer, final Double probability, final String predictedLabel) {
        this.containerNummer = containerNummer;
        this.probability = probability;
        this.predictedLabel = predictedLabel;
    }

    public void setContainerNummer(final Integer containerNummer) {
        this.containerNummer = containerNummer;
    }

    public void setProbability(final Double probability) {
        this.probability = probability;
    }

    public void setPredictedLabel(final String predictedLabel) {
        this.predictedLabel = predictedLabel;
    }

    public Integer getContainerNummer() {
        return containerNummer;
    }

    public Double getProbability() {
        return probability;
    }

    public String getPredictedLabel() {
        return predictedLabel;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ContainerPrediction that = (ContainerPrediction) o;
        return Objects.equals(containerNummer, that.containerNummer) &&
                Objects.equals(probability, that.probability) &&
                Objects.equals(predictedLabel, that.predictedLabel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(containerNummer, probability, predictedLabel);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ContainerPrediction{");
        sb.append("containerNummer='").append(containerNummer).append('\'');
        sb.append(", probability=").append(probability);
        sb.append(", predictedLabel=").append(predictedLabel);
        sb.append('}');
        return sb.toString();
    }
}
