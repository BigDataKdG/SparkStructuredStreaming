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

public class LedigingenModel implements Serializable {

    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm")
    private LocalDate startDate;


    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm")
    private LocalDate endDate;

    @JsonProperty("containerNummer")
    private String containerNummer;

    @JsonProperty("containerMeldingId")
    private String containerMeldingCategorie;

    @JsonProperty("dayOfWeek")
    private String dayOfWeek;

    public LedigingenModel() {
    }

    public void setStartDate(final LocalDate startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(final LocalDate endDate) {
        this.endDate = endDate;
    }

    public void setContainerNummer(final String containerNummer) {
        this.containerNummer = containerNummer;
    }

    public void setContainerMeldingCategorie(final String containerMeldingCategorie) {
        this.containerMeldingCategorie = containerMeldingCategorie;
    }

    public void setDayOfWeek(final String dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public String getContainerMeldingCategorie() {
        return containerMeldingCategorie;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public String getContainerNummer() {
        return containerNummer;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final LedigingenModel that = (LedigingenModel) o;
        return Objects.equals(startDate, that.startDate) &&
                Objects.equals(endDate, that.endDate) &&
                Objects.equals(containerNummer, that.containerNummer) &&
                Objects.equals(containerMeldingCategorie, that.containerMeldingCategorie) &&
                Objects.equals(dayOfWeek, that.dayOfWeek);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startDate, endDate, containerNummer, containerMeldingCategorie, dayOfWeek);
    }

    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder {
        private LocalDate startDate;
        private LocalDate endDate;
        private String containerNummer;
        private String containerMeldingCategorie;
        private String dayOfWeek;

        private Builder() {
        }

        public static Builder aLedigingenModel() {
            return new Builder();
        }

        public Builder startDate(LocalDate startDate) {
            this.startDate = startDate;
            return this;
        }

        public Builder endDate(LocalDate endDate) {
            this.endDate = endDate;
            return this;
        }

        public Builder containerNummer(String containerNummer) {
            this.containerNummer = containerNummer;
            return this;
        }

        public Builder containerMeldingCategorie(String containerMeldingCategorie) {
            this.containerMeldingCategorie = containerMeldingCategorie;
            return this;
        }

        public Builder dayOfWeek(String dayOfWeek) {
            this.dayOfWeek = dayOfWeek;
            return this;
        }

        public LedigingenModel build() {
            LedigingenModel ledigingenModel = new LedigingenModel();
            ledigingenModel.setStartDate(startDate);
            ledigingenModel.setEndDate(endDate);
            ledigingenModel.setContainerNummer(containerNummer);
            ledigingenModel.setContainerMeldingCategorie(containerMeldingCategorie);
            ledigingenModel.setDayOfWeek(dayOfWeek);
            return ledigingenModel;
        }
    }
}
