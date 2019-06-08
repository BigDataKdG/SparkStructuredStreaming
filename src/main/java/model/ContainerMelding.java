package model;

import java.time.LocalDateTime;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

public class ContainerMelding {

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm")
    private LocalDateTime containerActiviteit;

    @JsonProperty("containerNummer")
    private Integer containerNummer;

    @JsonProperty Integer containerMeldingId;

    @JsonProperty("dayOfWeek")
    private Integer dayOfWeek;

    public ContainerMelding(final LocalDateTime containerActiviteit,
                            final Integer containerNummer,
                            final Integer containerMeldingId,
                            final Integer dayOfWeek) {
        this.containerActiviteit = containerActiviteit;
        this.containerNummer = containerNummer;
        this.containerMeldingId = containerMeldingId;
        this.dayOfWeek = dayOfWeek;
    }

    public ContainerMelding() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public LocalDateTime getContainerActiviteit() {
        return containerActiviteit;
    }

    public Integer getContainerNummer() {
        return containerNummer;
    }

    public Integer getDayOfWeek() {
        return dayOfWeek;
    }

    public Integer getContainerMeldingId() {
        return containerMeldingId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ContainerMelding that = (ContainerMelding) o;
        return dayOfWeek == that.dayOfWeek &&
                Objects.equals(containerActiviteit, that.containerActiviteit) &&
                Objects.equals(containerNummer, that.containerNummer) &&
                Objects.equals(containerMeldingId, that.containerMeldingId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(containerActiviteit, containerNummer, containerMeldingId,dayOfWeek);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ContainerMelding{");
        sb.append("containerActiviteit=").append(containerActiviteit);
        sb.append(", containerNummer=").append(containerNummer);
        sb.append(", containerMeldingId=").append(containerMeldingId);
        sb.append(", dayOfWeek=").append(dayOfWeek);
        sb.append('}');
        return sb.toString();
    }


    public static final class Builder {
        Integer containerMeldingId;
        private LocalDateTime containerActiviteit;
        private Integer containerNummer;
        private Integer dayOfWeek;

        private Builder() {
        }

        public static Builder aContainerMelding() {
            return new Builder();
        }

        public Builder containerActiviteit(LocalDateTime containerActiviteit) {
            this.containerActiviteit = containerActiviteit;
            return this;
        }

        public Builder containerNummer(Integer containerNummer) {
            this.containerNummer = containerNummer;
            return this;
        }

        public Builder containerMeldingId(Integer containerMeldingId) {
            this.containerMeldingId = containerMeldingId;
            return this;
        }

        public Builder dayOfWeek(Integer dayOfWeek) {
            this.dayOfWeek = dayOfWeek;
            return this;
        }

        public ContainerMelding build() {
            return new ContainerMelding(containerActiviteit, containerNummer, containerMeldingId, dayOfWeek);
        }
    }
}
