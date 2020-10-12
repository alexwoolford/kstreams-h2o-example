package io.woolford.kafkairisdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class IrisConsumer {

    private final Logger LOG = LoggerFactory.getLogger(IrisConsumer.class);

    @KafkaListener(topics="iris", groupId = "iris-consumer")
    private void processMessage(String message) {
        LOG.info(message);
    }

}
