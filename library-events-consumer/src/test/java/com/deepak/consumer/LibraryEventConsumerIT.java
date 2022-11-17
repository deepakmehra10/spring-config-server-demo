package com.deepak.consumer;

import com.deepak.domain.Book;
import com.deepak.domain.LibraryEvent;
import com.deepak.domain.LibraryEventType;
import com.deepak.repository.LibraryEventRepository;
import com.deepak.service.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 1)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventConsumerIT {

    @Autowired
    EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @SpyBean
    LibraryEventService libraryEventService;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumer;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    private Consumer<Integer, String> consumer;

    @Value("${topics.retry}")
    String retryTopic;

    @BeforeEach
    public void setUp() {
    for (MessageListenerContainer messageListenerContainer: kafkaListenerEndpointRegistry.getListenerContainers()){
        ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
    }
    }

    @AfterEach
    public void tearDown() {
    libraryEventRepository.deleteAll();
    }
    @Test
    public void publishNewEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder().id(1).name("Microservices").author("Deepak").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .libraryEventType(LibraryEventType.NEW)
                .build();
        System.out.println(libraryEvent);
        String record = objectMapper.writeValueAsString(libraryEvent);
String json = "{\n" +
        "    \"libraryEventId\":6,\n" +
        "    \"libraryEventType\":\"NEW\",\n" +
        "    \"book\":{\n" +
        "        \"id\":\"1\",\n" +
        "        \"author\" :\"author\",\n" +
        "        \"name\":\"name\"\n" +
        "    }\n" +
        "}";
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class), isA(Acknowledgment.class));
        verify(libraryEventService, times(1)).persistLibraryEvent(isA(ConsumerRecord.class));
        List<LibraryEvent> all = (List<LibraryEvent>)libraryEventRepository.findAll();
        assertEquals(1, all.size());
    }

    @Test
    public void publishEventToRetryTopic() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\n" +
                "    \"libraryEventId\":100,\n" +
                "    \"libraryEventType\":\"NEW\",\n" +
                "    \"book\":{\n" +
                "        \"id\":\"1\",\n" +
                "        \"author\" :\"author\",\n" +
                "        \"name\":\"name\"\n" +
                "    }\n" +
                "}";
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

     verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class), isA(Acknowledgment.class));
       verify(libraryEventService, times(1)).persistLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafka));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        System.out.println("\n\n\n\n\n\n\n\n\n");
        System.out.println("retry " + retryTopic);
        System.out.println("\n\n\n\n\n\n\n\n\n");
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, retryTopic);
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
        System.out.println(consumerRecord.value());
        assertEquals(json, consumerRecord.value());
    }
}
