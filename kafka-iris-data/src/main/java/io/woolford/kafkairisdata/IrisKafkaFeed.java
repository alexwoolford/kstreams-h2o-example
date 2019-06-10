package io.woolford.kafkairisdata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;


@Component
@EnableScheduling
@EnableKafka
public class IrisKafkaFeed {

    @Autowired
    KafkaTemplate kafkaTemplate;

    private List<IrisRecord> irisRecordList;

    public IrisKafkaFeed() throws IOException {

        File csvFile = new File(getClass().getClassLoader().getResource("iris.csv").getFile());
        MappingIterator<IrisRecord> irisIter = new CsvMapper().readerWithTypedSchemaFor(IrisRecord.class).readValues(csvFile);
        this.irisRecordList = irisIter.readAll();

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