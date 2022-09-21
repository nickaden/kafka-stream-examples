package by.nick.kafka.streams.task;

import by.nick.kafka.streams.app.Printer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class Task3TopologyConfigTest {

    @Test
    void task3Topology() {
        Printer mockPrinter = mock(Printer.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        new Task3TopologyConfig().task3Topology(streamsBuilder, mockPrinter);

        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), streamProps);

        TestInputTopic<String, String> inputTopic1 = topologyTestDriver.createInputTopic(
                "task3_1", new StringSerializer(), new StringSerializer());
        TestInputTopic<String, String> inputTopic2 = topologyTestDriver.createInputTopic(
                "task3_2", new StringSerializer(), new StringSerializer());

        inputTopic1.pipeInput(null, "3:hello");
        inputTopic1.pipeInput(null, "4:hello");
        inputTopic2.pipeInput(null, "3:world");
        inputTopic2.pipeInput(null, "4:nick");
        inputTopic2.pipeInput(null, "3:again");

        verify(mockPrinter).print(anyString(), eq("3"), eq("hello world"));
        verify(mockPrinter).print(anyString(), eq("4"), eq("hello nick"));
        verify(mockPrinter).print(anyString(), eq("3"), eq("hello again"));
    }
}