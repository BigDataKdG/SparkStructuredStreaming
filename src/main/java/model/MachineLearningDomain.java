package model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MachineLearningDomain {

    @JsonProperty("containerNummer")
    private String containerNummer;

    @JsonProperty("volume")
    private Long volume;

    @JsonProperty("volumeSindsLaatsteLediging")
    private Long volumeSindsLaatsteLediging;

    @JsonProperty("dayOfWeek")
    private Integer dayOfWeek;

    public MachineLearningDomain(final String containerNummer, final Long volume,
                                 final Long volumeSindsLaatsteLediging, final Integer dayOfWeek) {
        this.containerNummer = containerNummer;
        this.volume = volume;
        this.volumeSindsLaatsteLediging = volumeSindsLaatsteLediging;
        this.dayOfWeek = dayOfWeek;
    }

    public String getContainerNummer() {
        return containerNummer;
    }

    public Long getVolume() {
        return volume;
    }

    public Long getVolumeSindsLaatsteLediging() {
        return volumeSindsLaatsteLediging;
    }

    public Integer getDayOfWeek() {
        return dayOfWeek;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MachineLearningDomain that = (MachineLearningDomain) o;
        return Objects.equals(containerNummer, that.containerNummer) &&
                Objects.equals(volume, that.volume) &&
                Objects.equals(volumeSindsLaatsteLediging, that.volumeSindsLaatsteLediging) &&
                Objects.equals(dayOfWeek, that.dayOfWeek);
    }

    @Override
    public int hashCode() {
        return Objects.hash(containerNummer, volume, volumeSindsLaatsteLediging, dayOfWeek);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MachineLearningDomain{");
        sb.append("containerNummer='").append(containerNummer).append('\'');
        sb.append(", volume=").append(volume);
        sb.append(", volumeSindsLaatsteLediging=").append(volumeSindsLaatsteLediging);
        sb.append(", dayOfWeek=").append(dayOfWeek);
        sb.append('}');
        return sb.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String containerNummer;
        private Long volume;
        private Long volumeSindsLaatsteLediging;
        private Integer dayOfWeek;

        private Builder() {
        }

        public static Builder aMachineLearningDomain() {
            return new Builder();
        }

        public Builder containerNummer(String containerNummer) {
            this.containerNummer = containerNummer;
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

        public Builder dayOfWeek(Integer dayOfWeek) {
            this.dayOfWeek = dayOfWeek;
            return this;
        }

        public MachineLearningDomain build() {
            return new MachineLearningDomain(containerNummer, volume, volumeSindsLaatsteLediging, dayOfWeek);
        }
    }
}
