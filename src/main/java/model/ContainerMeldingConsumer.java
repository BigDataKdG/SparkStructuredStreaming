package model;

import java.sql.Timestamp;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers;

public class ContainerMeldingConsumer {

    @JsonDeserialize(using = DateDeserializers.TimestampDeserializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Timestamp containerActiviteit;

    @JsonProperty("containerNummer")
    private Integer containerNummer;

    @JsonProperty("containerMeldingCategorie")
    private String containerMeldingCategorie;


    public ContainerMeldingConsumer(final Timestamp containerActiviteit,
                            final Integer containerNummer,
                            final String containerMeldingCategorie) {
        this.containerActiviteit = containerActiviteit;
        this.containerNummer = containerNummer;
        this.containerMeldingCategorie = containerMeldingCategorie;
    }

    public ContainerMeldingConsumer() {
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
        ContainerMeldingConsumer rhs = (ContainerMeldingConsumer) obj;
        return new EqualsBuilder()
                .append(this.containerActiviteit, rhs.containerActiviteit)
                .append(this.containerNummer, rhs.containerNummer)
                .append(this.containerMeldingCategorie, rhs.containerMeldingCategorie)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(containerActiviteit)
                .append(containerNummer)
                .append(containerMeldingCategorie)
                .toHashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ContainerMelding{");
        sb.append("containerActiviteit=").append(containerActiviteit);
        sb.append(", containerNummer=").append(containerNummer);
        sb.append(", containerMeldingCategorie='").append(containerMeldingCategorie).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Timestamp getContainerActiviteit() {
        return containerActiviteit;
    }

    public Integer getContainerNummer() {
        return containerNummer;
    }

    public String getContainerMeldingCategorie() {
        return containerMeldingCategorie;
    }

    public static final class Builder {
        private Timestamp containerActiviteit;
        private Integer containerNummer;
        private String containerMeldingCategorie;

        private Builder() {
        }

        public static Builder aContainer() {
            return new Builder();
        }

        public Builder containerActiviteit(Timestamp containerActiviteit) {
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

        public ContainerMeldingConsumer build() {
            return new ContainerMeldingConsumer(containerActiviteit, containerNummer, containerMeldingCategorie);
        }
    }
}
