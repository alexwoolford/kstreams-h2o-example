package io.woolford.kstreams.h2o.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import hex.genmodel.ModelMojoReader;
import hex.genmodel.MojoModel;
import hex.genmodel.MojoReaderBackend;
import hex.genmodel.MojoReaderBackendFactory;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.net.URL;
import java.util.Properties;

class IrisClassifier {

    final org.slf4j.Logger log = LoggerFactory.getLogger(IrisClassifier.class);

    EasyPredictModelWrapper modelWrapper;

    IrisClassifier() throws IOException {

        URL mojoSource = getClass().getClassLoader().getResource("DeepLearning_grid_1_AutoML_20190610_224939_model_2.zip");
        MojoReaderBackend reader = MojoReaderBackendFactory.createReaderBackend(mojoSource, MojoReaderBackendFactory.CachingStrategy.MEMORY);
        MojoModel model = ModelMojoReader.readFrom(reader);
        modelWrapper = new EasyPredictModelWrapper(model);

        for (String responseDomainValue: modelWrapper.getResponseDomainValues()){
            log.info("response domain value : " + responseDomainValue);
        }

    }

    void run() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> irisStream = builder.stream("iris");

        irisStream.map((key, value) -> {
            String classifiedValue = classifyIris(value);

            log.info("classifiedValue: " + classifiedValue);

            return new KeyValue<>(key, classifiedValue);
        }).to("iris-out-temp");

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private String classifyIris(String irisJson) {

        try {
            ObjectMapper mapper = new ObjectMapper();
            IrisRecord irisRecord = mapper.readValue(irisJson, IrisRecord.class);

            log.info("irisRecord (pre-classification): " + irisRecord);

            double sepal_length = irisRecord.getSepalLength();
            double sepal_width = irisRecord.getSepalWidth();
            double petal_length = irisRecord.getPetalLength();
            double petal_width = irisRecord.getPetalWidth();

            RowData row = new RowData();
            row.put("sepal_length", sepal_length);
            row.put("sepal_width", sepal_width);
            row.put("petal_length", petal_length);
            row.put("petal_width", petal_width);

            log.info("row: " + row);

            MultinomialModelPrediction prediction = (MultinomialModelPrediction) modelWrapper.predict(row);

            log.info("prediction.label: " + prediction.label);

            irisRecord.setPredictedSpecies(prediction.label);

            log.info("irisRecord (post-classification): " + irisRecord);

            irisJson = mapper.writeValueAsString(irisRecord);

            log.info("irisJson: " + irisJson);

        } catch (IOException|PredictException e) {
            log.error(e.getMessage());
        }

        return irisJson;

    }


}
