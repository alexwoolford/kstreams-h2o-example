package io.woolford.kafkairisdata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;


@Component
public class IrisKafkaFeed {

    @Autowired
    KafkaTemplate kafkaTemplate;

    private List<IrisRecord> irisRecordList;

    public IrisKafkaFeed() throws IOException {

        File csvFile = new File(getClass().getClassLoader().getResource("iris.csv").getFile());
        CsvSchema schema = CsvSchema.builder()
                .addColumn("sepalLength", CsvSchema.ColumnType.NUMBER)
                .addColumn("sepalWidth", CsvSchema.ColumnType.NUMBER)
                .addColumn("petalLength", CsvSchema.ColumnType.NUMBER)
                .addColumn("petalWidth", CsvSchema.ColumnType.NUMBER)
                .addColumn("species", CsvSchema.ColumnType.STRING)
                .build().withHeader();

        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<IrisRecord> irisIter = csvMapper.readerFor(IrisRecord.class).with(schema).readValues(csvFile);
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
