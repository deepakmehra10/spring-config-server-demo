package com.deepak.service;

import com.deepak.domain.LibraryEvent;
import com.deepak.domain.LibraryEventType;
import com.deepak.repository.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.deepak.domain.LibraryEventType.NEW;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    public void persistLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        System.out.println(consumerRecord.value());
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("LibraryEven: {}", libraryEvent);

        if (libraryEvent.getLibraryEventId() == 100) {
            System.out.println("\n\n\n\n\n\n\n\n\n");
            System.out.println("hello");
            System.out.println("\n\n\n\n\n\n\n\n\n");
            throw new IllegalArgumentException();
        }
        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
            default:
                log.info("Invalid EventType");


        }

    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("Successfully persisted the library event {}", libraryEvent);
    }
}