package com.yifeng.kafka_test.serializer;

import com.yifeng.kafka_test.pojo.Company;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by guoyifeng on 4/10/20
 */
public class CompanySerializer implements Serializer<Company> {

    private static final Logger LOG = LoggerFactory.getLogger(CompanySerializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // empty impl
    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        try {
            byte[] name, address; // byte arr for each field which needs serializing in Company
            if (data.getName() != null) {
                name = data.getName().getBytes("UTF-8");
            } else {
                name = new byte[0];
            }
            if (data.getAddress() != null) {
                address = data.getAddress().getBytes("UTF-8");
            } else {
                address = new byte[0];
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            byteBuffer.putInt(name.length);
            byteBuffer.put(name);
            byteBuffer.putInt(address.length);
            byteBuffer.put(address);
            return byteBuffer.array();
        } catch (UnsupportedEncodingException e) {
            LOG.error("error in serializing Company {}", e);
        }
        return null;
    }

    @Override
    public void close() {
        // empty impl
    }
}
