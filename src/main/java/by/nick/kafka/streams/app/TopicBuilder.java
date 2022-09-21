package by.nick.kafka.streams.app;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@RequiredArgsConstructor
public class TopicBuilder {

    private final KafkaAdmin kafkaAdmin;
    private final TopicListConfiguration topicListConfiguration;

    @PostConstruct
    public void setUpInfra() {
        topicListConfiguration.getTopic().forEach(topicConfiguration -> {
            Integer partitions = topicConfiguration.getPartitions() != null? topicConfiguration.getPartitions()
                    : topicListConfiguration.getDefaultPartitions();
            Short replications = topicConfiguration.getReplications() != null? topicConfiguration.getReplications()
                    : topicListConfiguration.getDefaultReplications();
            NewTopic newTopic = new NewTopic(topicConfiguration.getName(), partitions, replications);
            kafkaAdmin.createOrModifyTopics(newTopic);
        });
    }
}
