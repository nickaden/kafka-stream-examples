package by.nick.kafka.streams.task;

import by.nick.kafka.streams.app.Printer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class Task4TopologyConfigTest {

    @Test
    void tas3Topology() throws JsonProcessingException {
        Printer mockPrinter = mock(Printer.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        ObjectMapper objectMapper = new ObjectMapper();
        EmployeeSerDe employeeSerDe = new EmployeeSerDe(objectMapper);
        new Task4TopologyConfig().task4Topology(streamsBuilder, employeeSerDe, objectMapper,
                mockPrinter);

        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), streamProps);

        var employee = new Employee();
        employee.setName("John");
        employee.setCompany("Epam");
        employee.setPosition("Kafka Streams guru");
        employee.setExperience(4);
        TestInputTopic<String, Employee> inputTopic = topologyTestDriver.createInputTopic("task4",
                new StringSerializer(), employeeSerDe.serializer());
        inputTopic.pipeInput(null, employee);

        verify(mockPrinter).print(anyString(), any(), eq(objectMapper.writeValueAsString(employee)));
    }
}