package by.nick.kafka.streams.task;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Task1TopologyConfigTest {

    @Test
    void task1Stream() {
        StreamsBuilder builder = new StreamsBuilder();
        new Task1TopologyConfig().task1Stream(builder);
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamProps);

        TestInputTopic<String, String> inputTopic = topologyTestDriver.createInputTopic(
                "task1_1", new StringSerializer(), new StringSerializer());

        TestOutputTopic<String, String> outputTopic = topologyTestDriver.createOutputTopic(
                "task1_2", new StringDeserializer(), new StringDeserializer());

        String key = "key";
        String value = "value";
        inputTopic.pipeInput(key, value);
        TestRecord<String, String> testRecord = outputTopic.readRecord();
        assertEquals(key, testRecord.getKey());
        assertEquals(value, testRecord.getValue());
    }
}