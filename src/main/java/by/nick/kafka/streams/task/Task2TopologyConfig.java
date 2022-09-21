package by.nick.kafka.streams.task;

import by.nick.kafka.streams.app.Printer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class Task2TopologyConfig {

    @Bean
    public Map<String, KStream<Integer, String>> task2Words(StreamsBuilder streamsBuilder, Printer printer) {
        KStream<Integer, String> task2 = streamsBuilder.stream("task2");
        return task2.filter(((key, value) -> value != null))
                .flatMap(((key, value) -> splitByWords(value)))
                .peek((key, value) -> printer.print("Word record {}:{}", key, value))
                .split()
                .branch((key, value) -> key < 5, Branched.as("words-short"))
                .defaultBranch(Branched.as("words-long"));
    }

    @Bean
    public KStream<Integer, String> task2Output(Map<String, KStream<Integer, String>> task2Words, Printer printer) {
        task2Words.forEach((branchName, stream) ->stream.filter((key, value) -> value.contains("a"))
                        .foreach((key, value) -> printer.print(
                                "Got final message from {}: {}:{}", branchName,  key, value)));
        return task2Words.get("words-short");
    }

    private Iterable<KeyValue<Integer, String>> splitByWords(String value) {
        return Arrays.stream(value.split(StringUtils.SPACE))
                .map(word -> new KeyValue<>(word.length(), word))
                .collect(Collectors.toList());
    }
}
