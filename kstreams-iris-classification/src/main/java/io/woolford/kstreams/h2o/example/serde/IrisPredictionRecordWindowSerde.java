package io.woolford.kstreams.h2o.example.serde;

import io.woolford.kstreams.h2o.example.IrisClassifiedWindowRecord;
import org.apache.kafka.common.serialization.Serdes;

public class IrisPredictionRecordWindowSerde extends Serdes.WrapperSerde<IrisClassifiedWindowRecord> {
    public IrisPredictionRecordWindowSerde() {
        super(new JsonSerializer(), new JsonDeserializer(IrisClassifiedWindowRecord.class));
    }
}