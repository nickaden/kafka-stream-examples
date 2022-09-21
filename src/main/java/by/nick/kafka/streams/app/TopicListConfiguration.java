package by.nick.kafka.streams.app;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Data
@ConfigurationProperties(prefix = "app.kafka")
public class TopicListConfiguration {

    private List<TopicConfiguration> topic;
    private Integer defaultPartitions;
    private Short defaultReplications;

    @Data
    public static class TopicConfiguration {
        String name;
        Integer partitions;
        Short replications;
    }
}
