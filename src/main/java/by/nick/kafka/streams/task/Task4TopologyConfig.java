package by.nick.kafka.streams.task;

import by.nick.kafka.streams.app.Printer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Task4TopologyConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public KStream<String, Employee> task4Topology(StreamsBuilder streamsBuilder,
                                                   EmployeeSerDe employeeSerDe,
                                                   ObjectMapper objectMapper,
                                                   Printer printer) {
        KStream<String, Employee> stream = streamsBuilder.stream("task4", Consumed.with(Serdes.String(), employeeSerDe));
        stream.filter((key, value) -> value != null)
                .foreach((key, value) -> {
                    try {
                        printer.print("Got message {}:{}", key, objectMapper.writeValueAsString(value));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });

        return stream;
    }
}
