package by.nick.kafka.streams.task;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Task1TopologyConfig {

    @Bean
    public KStream<String, String> task1Stream(StreamsBuilder builder) {
        KStream<String, String> task1_1 = builder.stream("task1_1");
        task1_1.to("task1_2");
        return task1_1;
    }

}
