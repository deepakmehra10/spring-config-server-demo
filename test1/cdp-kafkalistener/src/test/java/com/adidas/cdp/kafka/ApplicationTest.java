package com.adidas.cdp.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import com.adidas.cdp.kafka.listener.KafkaListenerImpl;
import com.adidas.cdp.kafka.listener.KafkaRecord;


@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest
class ApplicationTest {

  private Acknowledgment ack;
  private KafkaRecord<String,String> kafkaRecord;

  @ClassRule
  public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1,
          false, 5, "response-topic");

  @Test
  void contextLoads() {
  }

  @BeforeClass
  public static void setup() {
    System.setProperty("spring.kafka.bootstrap-servers", broker.getEmbeddedKafka()
            .getBrokersAsString());
  }

  @Test
  public void listenTest() {

    String topic = "MyTopic";
    Collection<TopicPartition> partitions = new ArrayList<TopicPartition>();
    Collection<String> topicsCollection = new ArrayList<String>();
    partitions.add(new TopicPartition(topic, 1));
    Map<TopicPartition, Long> partitionsBeginningMap = new HashMap<TopicPartition, Long>();
    Map<TopicPartition, Long> partitionsEndMap = new HashMap<TopicPartition, Long>();

    long records = 10;
    for (TopicPartition partition : partitions) {
      partitionsBeginningMap.put(partition, 0l);
      partitionsEndMap.put(partition, records);
      topicsCollection.add(partition.topic());
    }

    MockConsumer<String, String> consumer = new MockConsumer<String, String>(
            OffsetResetStrategy.EARLIEST);
    consumer.subscribe(topicsCollection);
    consumer.rebalance(partitions);
    consumer.updateBeginningOffsets(partitionsBeginningMap);
    consumer.updateEndOffsets(partitionsEndMap);
    for (long i = 0; i < 10; i++) {
      String value = "myTest";
      ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
              topic, 1, i, null,value);
      consumer.addRecord(record);
    }

    KafkaListenerImpl<String, String> absCls = Mockito.mock(
            KafkaListenerImpl.class,
            Mockito.CALLS_REAL_METHODS);
    ConsumerRecords<String,String> consumerRecords = consumer.poll(100);

    for(ConsumerRecord<String,String> consumerRecord :consumerRecords){
      absCls.kafkaMessage(consumerRecord,ack);
      kafkaRecord = new KafkaRecord(consumerRecord.key(),consumerRecord.value(),
              consumerRecord.topic(),consumerRecord.partition(),consumerRecord.offset(),
              consumerRecord.headers());

      absCls.receive(kafkaRecord,ack);
      Assertions.assertNotNull(kafkaRecord);
      Mockito.verify(absCls).receive(kafkaRecord,ack);
    }

  }

}