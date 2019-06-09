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

    @JsonProperty("containerMeldingId")
    private String containerMeldingCategorie;

    @JsonProperty("dayOfWeek")
    private String dayOfWeek;

    @JsonProperty("count")
    private Long count;

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

    public String getDayOfWeek() {
        return dayOfWeek;
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

    public void setDayOfWeek(final String dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
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
                Objects.equals(containerMeldingCategorie, that.containerMeldingCategorie) &&
                Objects.equals(dayOfWeek, that.dayOfWeek) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, containerNummer, containerMeldingCategorie, dayOfWeek, count);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StortingenModel{");
        sb.append("date=").append(date);
        sb.append(", containerNummer='").append(containerNummer).append('\'');
        sb.append(", containerMeldingCategorie='").append(containerMeldingCategorie).append('\'');
        sb.append(", dayOfWeek='").append(dayOfWeek).append('\'');
        sb.append(", count=").append(count);
        sb.append('}');
        return sb.toString();
    }

    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder {
        private LocalDate date;
        private String containerNummer;
        private String containerMeldingCategorie;
        private String dayOfWeek;
        private Long count;

        private Builder() {
        }

        public static Builder aStortingenModel() {
            return new Builder();
        }

        public Builder date(LocalDate date) {
            this.date = date;
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

        public Builder count(Long count) {
            this.count = count;
            return this;
        }

        public StortingenModel build() {
            StortingenModel stortingenModel = new StortingenModel();
            stortingenModel.setDate(date);
            stortingenModel.setContainerNummer(containerNummer);
            stortingenModel.setContainerMeldingCategorie(containerMeldingCategorie);
            stortingenModel.setDayOfWeek(dayOfWeek);
            stortingenModel.setCount(count);
            return stortingenModel;
        }
    }
}
