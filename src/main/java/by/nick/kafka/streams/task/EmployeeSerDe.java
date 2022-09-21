package by.nick.kafka.streams.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
public class EmployeeSerDe implements Serde<Employee> {

    private final ObjectMapper objectMapper;

    @Override
    public Serializer<Employee> serializer() {
        return ((topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Deserializer<Employee> deserializer() {
        return ((topic, data) -> {
            try {
                return objectMapper.readValue(data, Employee.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
