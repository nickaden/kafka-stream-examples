package by.nick.kafka.streams.app;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Printer {

    public void print(String message, Object... placeholders) {
        log.info(message, placeholders);
    }
}

