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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

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

        // create and load default properties
        Properties props = new Properties();
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String propsPath = rootPath + "config.properties";
        FileInputStream in = new FileInputStream(propsPath);
        props.load(in);
        in.close();

        IrisRecordSerde irisRecordSerde = new IrisRecordSerde();

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, irisRecordSerde.getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        // create a stream of the raw iris records
        KStream<String, IrisRecord> irisStream = builder.stream("iris", Consumed.with(Serdes.String(), irisRecordSerde));

        // classify the raw iris messages with the classifyIris function.
        KStream<String, IrisRecord> irisStreamClassified = irisStream.mapValues(value -> {
            return classifyIris(value);
        });

        // write the classified records back to Kafka
        irisStreamClassified.to("iris-classified");

        // run it
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
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

}
