package io.woolford.kstreams.h2o.example.serde;

import io.woolford.kstreams.h2o.example.IrisRecord;
import org.apache.kafka.common.serialization.Serdes;

public class IrisRecordSerde extends Serdes.WrapperSerde<IrisRecord> {
    public IrisRecordSerde() {
        super(new JsonSerializer(), new JsonDeserializer(IrisRecord.class));
    }
}
