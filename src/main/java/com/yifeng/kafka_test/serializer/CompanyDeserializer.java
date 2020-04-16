package com.yifeng.kafka_test.serializer;

import com.yifeng.kafka_test.pojo.Company;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by guoyifeng on 4/10/20
 */
public class CompanyDeserializer implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
