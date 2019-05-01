package model;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ContainerMeldingDeserializer implements Deserializer<ContainerMelding> {

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {

    }

    @Override
    public ContainerMelding deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        ContainerMelding melding = null;
        try {
            melding = mapper.readValue(arg1, ContainerMelding.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return melding;
    }

}