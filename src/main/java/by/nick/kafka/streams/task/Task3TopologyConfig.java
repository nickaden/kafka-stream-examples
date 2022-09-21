package by.nick.kafka.streams.task;

import by.nick.kafka.streams.app.Printer;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.text.MessageFormat;
import java.time.Duration;

@Configuration
public class Task3TopologyConfig {

    @Bean
    public KStream<String, String> task3Topology(StreamsBuilder streamsBuilder, Printer printer) {
        KStream<String, String> stream1 = updateWithCommonPipeline(streamsBuilder.stream("task3_1"));
        KStream<String, String> stream2 = updateWithCommonPipeline(streamsBuilder.stream("task3_2"));
        stream1.join(stream2, (left, right) -> MessageFormat.format("{0} {1}", left, right),
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30)))
                .foreach((key, value) -> printer.print("Got message: {}:{}", key, value));
        return stream1;
    }

    private KStream<String, String> updateWithCommonPipeline(KStream<String, String> stream) {
        return stream.filter(((key, value) -> value != null))
                .filter(((key, value) -> value.contains(":")))
                .map((key, value) -> {
                    var splited = value.split(":");
                    return new KeyValue<>(splited[0], splited[1]);
                });
    }
}
