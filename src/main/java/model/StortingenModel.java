package model;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;

public class StortingenModel implements Serializable {

    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm")
    private LocalDate date;

    @JsonProperty("containerNummer")
    private String containerNummer;

    @JsonProperty("count")
    private Long count;

    @JsonProperty("containerMeldingCategorie")
    private String containerMeldingCategorie;


    public StortingenModel() {
    }

    public LocalDate getDate() {
        return date;
    }

    public String getContainerNummer() {
        return containerNummer;
    }

    public Long getCount() {
        return count;
    }

    public String getContainerMeldingCategorie() {
        return containerMeldingCategorie;
    }

    public void setDate(final LocalDate date) {
        this.date = date;
    }

    public void setContainerNummer(final String containerNummer) {
        this.containerNummer = containerNummer;
    }

    public void setCount(final Long count) {
        this.count = count;
    }

    public void setContainerMeldingCategorie(final String containerMeldingCategorie) {
        this.containerMeldingCategorie = containerMeldingCategorie;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StortingenModel{");
        sb.append("date=").append(date);
        sb.append(", containerNummer='").append(containerNummer).append('\'');
        sb.append(", count=").append(count);
        sb.append(", containerMeldingCategorie='").append(containerMeldingCategorie).append('\'');
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
        final StortingenModel that = (StortingenModel) o;
        return Objects.equals(date, that.date) &&
                Objects.equals(containerNummer, that.containerNummer) &&
                Objects.equals(count, that.count) &&
                Objects.equals(containerMeldingCategorie, that.containerMeldingCategorie);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, containerNummer, count, containerMeldingCategorie);
    }

    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder {
        private LocalDate window;
        private String container_nr;
        private Long count;
        private String containerMeldingCategorie;

        private Builder() {
        }

        public static Builder aMachineLearningModel() {
            return new Builder();
        }

        public Builder window(LocalDate window) {
            this.window = window;
            return this;
        }

        public Builder container_nr(String container_nr) {
            this.container_nr = container_nr;
            return this;
        }

        public Builder count(Long count) {
            this.count = count;
            return this;
        }

        public Builder containerMeldingCategorie(String containerMeldingCategorie) {
            this.containerMeldingCategorie = containerMeldingCategorie;
            return this;
        }

        public StortingenModel build() {
            StortingenModel stortingenModel = new StortingenModel();
            stortingenModel.setDate(window);
            stortingenModel.setContainerNummer(container_nr);
            stortingenModel.setCount(count);
            stortingenModel.setContainerMeldingCategorie(containerMeldingCategorie);
            return stortingenModel;
        }
    }
}
