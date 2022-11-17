package com.adidias.cdp.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Component
public class KafkaProducerImplClass<K,V> {

  /*@KafkaListener(topics = "${spring.kafka.consumer.topic}",
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
  }*/

  @Autowired
  KafkaTemplate<K,V> kafkaTemplate;

  public void sendToTopic(ProducerRecord<K, V> producerRecord)
  {
    ListenableFuture<SendResult<K, V>> future = kafkaTemplate.send(producerRecord);
    future.addCallback(new ListenableFutureCallback() {
      @Override
      public void onFailure(Throwable ex) {
        log.error("Message push to topic failed");
      }

      @Override
      public void onSuccess(Object result) {
        log.info("Message push to topic successful");
      }
    });

  }


  /*public abstract void receive(KafkaRecord<K, V> kafkaRecord, Acknowledgment ack);*/

}
