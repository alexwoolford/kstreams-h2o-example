package io.woolford.kstreams.h2o.example.serde;

import io.woolford.kstreams.h2o.example.IrisClassifiedRecord;
import org.apache.kafka.common.serialization.Serdes;

public class IrisPredictionRecordSerde extends Serdes.WrapperSerde<IrisClassifiedRecord> {
    public IrisPredictionRecordSerde() {
        super(new JsonSerializer(), new JsonDeserializer(IrisClassifiedRecord.class));
    }
}