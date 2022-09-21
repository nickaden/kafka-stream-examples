package by.nick.kafka.streams.task;

import by.nick.kafka.streams.app.Printer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class Task2TopologyConfigTest {

    @Test
    void task2Words() {
        Printer mockPrinter = mock(Printer.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        Map<String, KStream<Integer, String>> stringKStreamMap = new Task2TopologyConfig().task2Words(streamsBuilder, mockPrinter);
        stringKStreamMap.forEach((branch, stream) -> stream.foreach((key, value) -> {}));
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), streamProps);

        TestInputTopic<String, String> input = topologyTestDriver.createInputTopic(
                "task2", new StringSerializer(), new StringSerializer());
        input.pipeInput(null, "test teeth old newly");

        verify(mockPrinter).print(anyString(), eq(4), eq("test"));
        verify(mockPrinter).print(anyString(), eq(5), eq("teeth"));
        verify(mockPrinter).print(anyString(), eq(3), eq("old"));
        verify(mockPrinter).print(anyString(), eq(5), eq("newly"));
    }

    @Test
    void task2Output() {
        Printer mockPrinter = mock(Printer.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var map = Map.of(
                "topic-1", streamsBuilder.stream("test-topic-1", Consumed.with(Serdes.Integer(), Serdes.String())),
                "topic-2", streamsBuilder.stream("test-topic-2", Consumed.with(Serdes.Integer(), Serdes.String()))
        );
        new Task2TopologyConfig().task2Output(map, mockPrinter);
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), streamProps);

        TestInputTopic<Integer, String> inputTopic1 = topologyTestDriver.createInputTopic(
                "test-topic-1", new IntegerSerializer(), new StringSerializer());
        TestInputTopic<Integer, String> inputTopic2 = topologyTestDriver.createInputTopic(
                "test-topic-2", new IntegerSerializer(), new StringSerializer());

        inputTopic1.pipeInput(4, "word");
        inputTopic2.pipeInput(5, "hello");
        inputTopic1.pipeInput(3, "ash");
        inputTopic2.pipeInput(4, "anne");

        verify(mockPrinter).print(anyString(), eq("topic-1"), eq(3), eq("ash"));
        verify(mockPrinter).print(anyString(), eq("topic-2"), eq(4), eq("anne"));
    }
}