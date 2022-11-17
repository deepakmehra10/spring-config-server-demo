package com.deepak.consumer;

import com.deepak.service.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {

    @Autowired
    LibraryEventService libraryEventService;

    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) throws JsonProcessingException {
        log.info("ConsumerRecord: {} ", consumerRecord);
        System.out.println("\n\n\n\n\n\n\n");
        System.out.println("kya idhar aa ra hun");
        System.out.println("\n\n\n\n\n\n\n");
        libraryEventService.persistLibraryEvent(consumerRecord);
        acknowledgment.acknowledge();
    }
}
