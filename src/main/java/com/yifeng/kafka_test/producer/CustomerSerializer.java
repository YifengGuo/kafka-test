package com.yifeng.kafka_test.producer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by guoyifeng on 10/12/18
 */
public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        return;
    }

    /**
     * to serialize Customer object
     * use 4 bytes int for customerId
     * use 4 bytes int for length of customerName in UTF-8 bytes
     * N bytes for customerName in UTF-8
     */
    @Override
    public byte[] serialize(String topic, Customer customer) {
        try {
            byte[] serializedName;
            int stringSize;
            if (customer == null) {
                return null;
            } else {
                if (customer.getCustomerName() != null) {
                    serializedName = customer.getCustomerName().getBytes("UTF-8");
                    stringSize = serializedName.length;
                } else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + stringSize);
            byteBuffer.putInt(customer.getCustomerId());
            byteBuffer.putInt(stringSize);
            byteBuffer.put(serializedName);
            return byteBuffer.array();
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException(("Error when serializing Customer to byte[]" + e));
        }
    }

    @Override
    public void close() {
        return;
    }
}
