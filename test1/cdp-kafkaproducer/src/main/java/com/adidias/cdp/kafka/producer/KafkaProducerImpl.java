package com.adidias.cdp.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaProducerImpl<K,V> {

  KafkaProducerImplClass<Object,Object> kafkaProducer = new KafkaProducerImplClass<>();

  public default void sendToTopic(ProducerRecord<Object, Object> producerRecord)
  {
    kafkaProducer.sendToTopic(producerRecord);
  }
}
