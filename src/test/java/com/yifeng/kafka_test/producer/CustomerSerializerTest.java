package com.yifeng.kafka_test.producer;

import com.yifeng.kafka_test.producer.Customer;
import com.yifeng.kafka_test.producer.CustomerSerializer;
import org.junit.Test;

/**
 * Created by guoyifeng on 10/12/18
 */
public class CustomerSerializerTest {
    @Test
    public void testCustomizedSerializer() {
        Customer customer = new Customer(1, "William West");
        CustomerSerializer serializer = new CustomerSerializer();
        byte[] bytes = serializer.serialize("1", customer);
        for (byte b : bytes) {
            System.out.print(b + " ");
        }
    }
}
