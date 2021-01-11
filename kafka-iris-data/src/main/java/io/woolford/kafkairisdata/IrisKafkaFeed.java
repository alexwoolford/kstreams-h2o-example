package io.woolford.kafkairisdata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;


@Component
public class IrisKafkaFeed {

    private final Logger logger = LoggerFactory.getLogger(IrisKafkaFeed.class);

    @Autowired
    KafkaTemplate kafkaTemplate;

    private List<IrisRecord> irisRecordList;

    public IrisKafkaFeed() throws IOException {

        String irisCsvString = "";
        ClassPathResource cpr = new ClassPathResource("iris.csv");
        try {
            byte[] bdata = FileCopyUtils.copyToByteArray(cpr.getInputStream());
            irisCsvString = new String(bdata, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.warn("IOException", e);
        }

        CsvSchema schema = CsvSchema.builder()
                .addColumn("sepalLength", CsvSchema.ColumnType.NUMBER)
                .addColumn("sepalWidth", CsvSchema.ColumnType.NUMBER)
                .addColumn("petalLength", CsvSchema.ColumnType.NUMBER)
                .addColumn("petalWidth", CsvSchema.ColumnType.NUMBER)
                .addColumn("species", CsvSchema.ColumnType.STRING)
                .build().withHeader();

        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<IrisRecord> irisIter = csvMapper.readerFor(IrisRecord.class).with(schema).readValues(irisCsvString);
        this.irisRecordList = irisIter.readAll();

    }

    @Bean
    public NewTopic createIrisTopic() {
        return TopicBuilder.name("iris")
                .partitions(1)
                .replicas(3)
                .build();
    }

    @Scheduled(fixedDelay = 1000L)
    private void publishIrisRecord() throws JsonProcessingException {

        Random rand = new Random();
        IrisRecord irisRecord = irisRecordList.get(rand.nextInt(irisRecordList.size()));

        ObjectMapper mapper = new ObjectMapper();
        String irisRecordJson = mapper.writeValueAsString(irisRecord);

        kafkaTemplate.send("iris", irisRecordJson);

    }

}
