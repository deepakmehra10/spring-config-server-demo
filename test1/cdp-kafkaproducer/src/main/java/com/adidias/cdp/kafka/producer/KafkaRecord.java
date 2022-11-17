package com.adidias.cdp.kafka.producer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.header.Headers;

/**
 * Specific Record for Kafka consumer message instance
 *
 * @param <K> Key for kafka message
 * @param <V> Value of Kafka message
 */

@Getter
@Setter
@AllArgsConstructor
public final class KafkaRecord<K,V>{
  public static final int NULL_CHECKSUM = -1;
  
  private K key;
  private V value;

  private String topic;
  private int partition;
  private long offset;
  private Headers headers;
}
