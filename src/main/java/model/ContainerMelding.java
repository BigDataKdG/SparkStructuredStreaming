package model;

import java.time.LocalDate;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;

public class ContainerMelding {

    @JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private LocalDate containerActiviteit;

    @JsonProperty("containerNummer")
    private Integer containerNummer;

    @JsonProperty("containerMeldingCategorie")
    private String containerMeldingCategorie;

    @JsonProperty("afvaltype")
    private String afvaltype;

    public ContainerMelding(final LocalDate containerActiviteit,
                            final Integer containerNummer,
                            final String containerMeldingCategorie,
                            final String afvaltype) {
        this.containerActiviteit = containerActiviteit;
        this.containerNummer = containerNummer;
        this.containerMeldingCategorie = containerMeldingCategorie;
        this.afvaltype = afvaltype;
    }

    public ContainerMelding() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ContainerMelding rhs = (ContainerMelding) obj;
        return new EqualsBuilder()
                .append(this.containerActiviteit, rhs.containerActiviteit)
                .append(this.containerNummer, rhs.containerNummer)
                .append(this.containerMeldingCategorie, rhs.containerMeldingCategorie)
                .append(this.afvaltype, rhs.afvaltype)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(containerActiviteit)
                .append(containerNummer)
                .append(containerMeldingCategorie)
                .append(afvaltype)
                .toHashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ContainerMelding{");
        sb.append("containerActiviteit=").append(containerActiviteit);
        sb.append(", containerNummer=").append(containerNummer);
        sb.append(", containerMeldingCategorie='").append(containerMeldingCategorie).append('\'');
        sb.append(", afvaltype=").append(afvaltype);
        sb.append('}');
        return sb.toString();
    }

    public LocalDate getContainerActiviteit() {
        return containerActiviteit;
    }

    public Integer getContainerNummer() {
        return containerNummer;
    }

    public String getContainerMeldingCategorie() {
        return containerMeldingCategorie;
    }

    public String getAfvaltype() {
        return afvaltype;
    }

    public static final class Builder {
        private LocalDate containerActiviteit;
        private Integer containerNummer;
        private String containerMeldingCategorie;
        private String afvaltype;

        private Builder() {
        }

        public static Builder aContainer() {
            return new Builder();
        }

        public Builder containerActiviteit(LocalDate containerActiviteit) {
            this.containerActiviteit = containerActiviteit;
            return this;
        }

        public Builder containerNummer(Integer containerNummer) {
            this.containerNummer = containerNummer;
            return this;
        }

        public Builder containerMeldingCategorie(String containerMeldingCategorie) {
            this.containerMeldingCategorie = containerMeldingCategorie;
            return this;
        }

        public Builder afvaltype(String afvaltype) {
            this.afvaltype = afvaltype;
            return this;
        }

        public ContainerMelding build() {
            return new ContainerMelding(containerActiviteit, containerNummer, containerMeldingCategorie, afvaltype);
        }
    }
}
