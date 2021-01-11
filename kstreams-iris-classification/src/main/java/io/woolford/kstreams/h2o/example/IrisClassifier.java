package io.woolford.kstreams.h2o.example;

import hex.genmodel.ModelMojoReader;
import hex.genmodel.MojoModel;
import hex.genmodel.MojoReaderBackend;
import hex.genmodel.MojoReaderBackendFactory;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import io.woolford.kstreams.h2o.example.serde.IrisRecordSerde;
import io.woolford.kstreams.h2o.example.serde.IrisPredictionRecordSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.*;

class IrisClassifier {

    final Logger LOG = LoggerFactory.getLogger(IrisClassifier.class);

    // The IrisClassifier constructor reads in the MOJO
    EasyPredictModelWrapper modelWrapper;
    IrisClassifier() throws IOException {
        URL mojoSource = getClass().getClassLoader().getResource("DeepLearning_grid_1_AutoML_20190610_224939_model_2.zip");
        MojoReaderBackend reader = MojoReaderBackendFactory.createReaderBackend(mojoSource, MojoReaderBackendFactory.CachingStrategy.MEMORY);
        MojoModel model = ModelMojoReader.readFrom(reader);
        modelWrapper = new EasyPredictModelWrapper(model);
    }

    void run() throws IOException {

        // load properties
        Properties props = new Properties();
        InputStream input = IrisClassifier.class.getClassLoader().getResourceAsStream("config.properties");
        props.load(input);

        IrisRecordSerde irisRecordSerde = new IrisRecordSerde();
        IrisPredictionRecordSerde irisPredictionRecordSerde = new IrisPredictionRecordSerde();

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, irisPredictionRecordSerde.getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, irisRecordSerde.getClass());

        // create the topics
        createTopics(props);

        final StreamsBuilder builder = new StreamsBuilder();

        // create a stream of the raw iris records
        KStream<IrisClassifiedRecord, IrisRecord> irisStream = builder.stream("iris", Consumed.with(irisPredictionRecordSerde, irisRecordSerde));

        // classify the raw iris messages with the classifyIris function.
        KStream<IrisClassifiedRecord, IrisRecord> irisStreamClassified = irisStream.map((k, v) -> {

            // predict the species
            IrisRecord irisRecord = classifyIris(v);

            // create actual/predicted record
            IrisClassifiedRecord irisClassifiedRecord = new IrisClassifiedRecord();
            irisClassifiedRecord.setSpecies(irisRecord.getSpecies());
            irisClassifiedRecord.setPredictedSpecies(irisRecord.getPredictedSpecies());

            return new KeyValue<>(irisClassifiedRecord, classifyIris(v));
        });

        // write the classified records back to Kafka
        irisStreamClassified.to("iris-classified");

        // create one-minute tumbling windows containing actual/predicted counts
        KTable<Windowed<IrisClassifiedRecord>, Long> irisClassifiedWindowCounts = irisStreamClassified
                .selectKey((k, v) -> k)
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count();

        // write the windowed counts to iris-classified-window topic
        irisClassifiedWindowCounts.toStream().map((k, v) -> {
            IrisClassifiedWindowRecord irisClassifiedWindowRecord = new IrisClassifiedWindowRecord();
            irisClassifiedWindowRecord.setSpecies(k.key().getSpecies());
            irisClassifiedWindowRecord.setPredictedSpecies(k.key().getPredictedSpecies());
            irisClassifiedWindowRecord.setStartMs(k.window().start());
            irisClassifiedWindowRecord.setEndMs(k.window().end());
            irisClassifiedWindowRecord.setCount(v);
            return new KeyValue(null, irisClassifiedWindowRecord);
        }).to("iris-classified-window-counts");

        //TODO: add topic with correct classification percentage

        // run it
        final Topology topology = builder.build();

        // show topology
        LOG.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private IrisRecord classifyIris(IrisRecord irisRecord) {

        try {
            RowData row = new RowData();
            row.put("sepal_length", irisRecord.getSepalLength());
            row.put("sepal_width", irisRecord.getSepalWidth());
            row.put("petal_length", irisRecord.getPetalLength());
            row.put("petal_width", irisRecord.getPetalWidth());

            MultinomialModelPrediction prediction = (MultinomialModelPrediction) modelWrapper.predict(row);
            irisRecord.setPredictedSpecies(prediction.label);

        } catch (PredictException e) {
            LOG.error(e.getMessage());
        }

        return irisRecord;

    }

    private void createTopics(Properties props){

        AdminClient client = AdminClient.create(props);

        List<NewTopic> topics = new ArrayList<>();
        for (String topicName : ((String) props.get("topics")).split(",")){
            NewTopic topic = new NewTopic(topicName, 1, (short) 3);
            topics.add(topic);
        }

        client.createTopics(topics);
        client.close();

    }

}
