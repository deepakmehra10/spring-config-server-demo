package com.adidas.cdp.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

public abstract class KafkaListenerImpl<K,V> {

  @KafkaListener(topics = "${spring.kafka.consumer.topic}",
          concurrency = "${spring.kafka.concurrency}",
          containerFactory = "concurrentKafkaListenerContainerFactory")
  public void kafkaMessage(ConsumerRecord<K,V> message, Acknowledgment ack) {
    KafkaRecord kafkaRecord = new KafkaRecord(message.key(),
        message.value(),
        message.topic(),
        message.partition(),
        message.offset(),
        message.headers());
    receive(kafkaRecord, ack);
  }


  public abstract void receive(KafkaRecord<K, V> kafkaRecord, Acknowledgment ack);

}
