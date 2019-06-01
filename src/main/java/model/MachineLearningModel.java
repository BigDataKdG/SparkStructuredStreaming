package model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MachineLearningModel {


    @JsonProperty("containerNummer")
    private String containerNummer;

    @JsonProperty("afvaltype")
    private String afvaltype;

    @JsonProperty("volume")
    private Long volume;

    @JsonProperty("volumeSindsLaatsteLediging")
    private Long volumeSindsLaatsteLediging;

    @JsonProperty("verplicht")
    private Integer verplicht;

    @JsonProperty("aantalInwoners")
    private Integer aantalInwoners;

    @JsonProperty("dagWeek")
    private Integer dagWeek;

    @JsonProperty("ledigingDag")
    private Integer ledigingDag;

    public MachineLearningModel(final String containerNummer, final String afvaltype, final Long volume,
                                final Long volumeSindsLaatsteLediging, final Integer verplicht,
                                final Integer aantalInwoners, final Integer dagWeek, final Integer ledigingDag) {
        this.containerNummer = containerNummer;
        this.afvaltype = afvaltype;
        this.volume = volume;
        this.volumeSindsLaatsteLediging = volumeSindsLaatsteLediging;
        this.verplicht = verplicht;
        this.aantalInwoners = aantalInwoners;
        this.dagWeek = dagWeek;
        this.ledigingDag = ledigingDag;
    }

    public String getContainerNummer() {
        return containerNummer;
    }

    public String getAfvaltype() {
        return afvaltype;
    }

    public Long getVolume() {
        return volume;
    }

    public Long getVolumeSindsLaatsteLediging() {
        return volumeSindsLaatsteLediging;
    }

    public Integer getVerplicht() {
        return verplicht;
    }

    public Integer getAantalInwoners() {
        return aantalInwoners;
    }

    public Integer getDagWeek() {
        return dagWeek;
    }

    public Integer getLedigingDag() {
        return ledigingDag;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MachineLearningModel{");
        sb.append("containerNummer='").append(containerNummer).append('\'');
        sb.append(", afvaltype='").append(afvaltype).append('\'');
        sb.append(", volume=").append(volume);
        sb.append(", volumeSindsLaatsteLediging=").append(volumeSindsLaatsteLediging);
        sb.append(", verplicht=").append(verplicht);
        sb.append(", aantalInwoners=").append(aantalInwoners);
        sb.append(", dagWeek=").append(dagWeek);
        sb.append(", ledigingDag=").append(ledigingDag);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MachineLearningModel that = (MachineLearningModel) o;
        return Objects.equals(containerNummer, that.containerNummer) &&
                Objects.equals(afvaltype, that.afvaltype) &&
                Objects.equals(volume, that.volume) &&
                Objects.equals(volumeSindsLaatsteLediging, that.volumeSindsLaatsteLediging) &&
                Objects.equals(verplicht, that.verplicht) &&
                Objects.equals(aantalInwoners, that.aantalInwoners) &&
                Objects.equals(dagWeek, that.dagWeek) &&
                Objects.equals(ledigingDag, that.ledigingDag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(containerNummer, afvaltype, volume, volumeSindsLaatsteLediging, verplicht, aantalInwoners, dagWeek, ledigingDag);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String containerNummer;
        private String afvaltype;
        private Long volume;
        private Long volumeSindsLaatsteLediging;
        private Integer verplicht;
        private Integer aantalInwoners;
        private Integer dagWeek;
        private Integer ledigingDag;

        private Builder() {
        }

        public static Builder aMachineLearningModel() {
            return new Builder();
        }

        public Builder containerNummer(String containerNummer) {
            this.containerNummer = containerNummer;
            return this;
        }

        public Builder afvaltype(String afvaltype) {
            this.afvaltype = afvaltype;
            return this;
        }

        public Builder volume(Long volume) {
            this.volume = volume;
            return this;
        }

        public Builder volumeSindsLaatsteLediging(Long volumeSindsLaatsteLediging) {
            this.volumeSindsLaatsteLediging = volumeSindsLaatsteLediging;
            return this;
        }

        public Builder verplicht(Integer verplicht) {
            this.verplicht = verplicht;
            return this;
        }

        public Builder aantalInwoners(Integer aantalInwoners) {
            this.aantalInwoners = aantalInwoners;
            return this;
        }

        public Builder dagWeek(Integer dagWeek) {
            this.dagWeek = dagWeek;
            return this;
        }

        public Builder ledigingDag(Integer ledigingDag) {
            this.ledigingDag = ledigingDag;
            return this;
        }

        public MachineLearningModel build() {
            return new MachineLearningModel(containerNummer, afvaltype, volume, volumeSindsLaatsteLediging, verplicht, aantalInwoners, dagWeek, ledigingDag);
        }
    }
}
