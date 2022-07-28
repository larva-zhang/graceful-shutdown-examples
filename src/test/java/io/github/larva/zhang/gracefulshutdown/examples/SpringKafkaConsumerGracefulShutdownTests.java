package io.github.larva.zhang.gracefulshutdown.examples;

import io.github.larva.zhang.gracefulshutdown.examples.SpringKafkaConsumerGracefulShutdownTests.KafkaTestConfig;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Resource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.event.ConsumerResumedEvent;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * SpringKafkaConsumerGracefulShutdownTests
 *
 * @author larva-zhang
 * @date 2022/7/27
 * @since 1.0
 */
@EnableKafka
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = KafkaTestConfig.class)
@EmbeddedKafka(partitions = 1, brokerProperties = {
    "listeners=PLAINTEXT://localhost:3333",
    "port=3333"
})
public class SpringKafkaConsumerGracefulShutdownTests {

    private static final String DEFAULT_TOPIC = "templateTopic";

    private static final String GROUP_ID = "test-consumer-group";

    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 500;

    private static final AtomicInteger CONSUMER_COUNTER = new AtomicInteger();

    private static volatile boolean receiveConsumerResumedEvent;
    private static volatile boolean receiveConsumerPausedEvent;
    private static volatile boolean receiveConsumerStoppedEvent;
    private static volatile boolean receiveConsumerStartedEvent;
    private volatile ConsumerRecord<Integer, String> lastRecord = new ConsumerRecord<>(
        DEFAULT_TOPIC, 1, -1, -1, "init record");

    @Resource
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Resource
    private KafkaTemplate<Integer, String> kafkaTemplate;


    @KafkaListener(id = GROUP_ID, topics = DEFAULT_TOPIC, autoStartup = "false")
    public void KafkaListener(ConsumerRecord<Integer, String> consumerRecord) {
        System.out.println(consumerRecord);
        lastRecord = consumerRecord;
    }

    @Test
    public void testSpringKafkaConsumerGracefulShutdown() throws InterruptedException {
        testConsumerAutoStartupIgnoreKafkaListenerAutoStartupPropertySetFalse();
        testConsumerPause();
        testConsumerResume();
        testConsumerStop();
        testConsumerStart();
        testStartPauseRequestedContainerAndExceptConsumerPollRecordsPaused();
    }

    private void testConsumerAutoStartupIgnoreKafkaListenerAutoStartupPropertySetFalse() {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(
            GROUP_ID);
        Assert.assertNotNull(listenerContainer);
        Assert.assertFalse(listenerContainer.isAutoStartup());
        // except listener container auto startup and ignore @KafkaListener autoStartup property is false
        Assert.assertTrue(listenerContainer.isRunning());
        Assert.assertTrue(receiveConsumerStartedEvent);
        Assert.assertEquals(1, CONSUMER_COUNTER.get());
        String recordValue = "testConsumerAutoStartup";
        kafkaTemplate.send(DEFAULT_TOPIC, recordValue);
        awaitForNextPoll(listenerContainer);
        Assert.assertEquals(recordValue, lastRecord.value());
    }

    private void testConsumerPause() throws InterruptedException {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(
            GROUP_ID);
        Assert.assertNotNull(listenerContainer);
        Assert.assertTrue(listenerContainer.isRunning());
        Assert.assertFalse(listenerContainer.isPauseRequested());
        Assert.assertFalse(listenerContainer.isContainerPaused());
        // except listener container poll records paused
        receiveConsumerPausedEvent = false;
        listenerContainer.pause();
        while (!receiveConsumerPausedEvent) {
            TimeUnit.SECONDS.sleep(1);
        }
        Assert.assertEquals(1, CONSUMER_COUNTER.get());
        Assert.assertTrue(listenerContainer.isRunning());
        Assert.assertTrue(listenerContainer.isContainerPaused());
        String recordValue = "testConsumerPause";
        kafkaTemplate.send(DEFAULT_TOPIC, recordValue);
        awaitForNextPoll(listenerContainer);
        Assert.assertNotEquals(recordValue, lastRecord.value());
    }

    private void testConsumerResume() throws InterruptedException {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(
            GROUP_ID);
        Assert.assertNotNull(listenerContainer);
        Assert.assertTrue(listenerContainer.isRunning());
        Assert.assertTrue(listenerContainer.isContainerPaused());
        Assert.assertTrue(listenerContainer.isPauseRequested());
        // except consumer resume poll records
        receiveConsumerResumedEvent = false;
        listenerContainer.resume();
        while (!receiveConsumerResumedEvent) {
            TimeUnit.SECONDS.sleep(1);
        }
        Assert.assertEquals(1, CONSUMER_COUNTER.get());
        Assert.assertTrue(listenerContainer.isRunning());
        Assert.assertFalse(listenerContainer.isContainerPaused());
        String recordValue = "testConsumerResume";
        kafkaTemplate.send(DEFAULT_TOPIC, recordValue);
        awaitForNextPoll(listenerContainer);
        Assert.assertEquals(recordValue, lastRecord.value());
    }

    private void testConsumerStop() throws InterruptedException {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(
            GROUP_ID);
        Assert.assertNotNull(listenerContainer);
        Assert.assertTrue(listenerContainer.isRunning());
        // except container stopped and consumer removed and poll records stopped
        receiveConsumerStoppedEvent = false;
        listenerContainer.stop();
        while (!receiveConsumerStoppedEvent) {
            TimeUnit.SECONDS.sleep(1);
        }
        Assert.assertEquals(0, CONSUMER_COUNTER.get());
        Assert.assertFalse(listenerContainer.isRunning());
        String recordValue = "testConsumerStop";
        long oldOffset = lastRecord.offset();
        kafkaTemplate.send(DEFAULT_TOPIC, recordValue);
        awaitForNextPoll(listenerContainer);
        Assert.assertNotEquals(recordValue, lastRecord.value());
        Assert.assertEquals(oldOffset, lastRecord.offset());
    }

    private void testConsumerStart() throws InterruptedException {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(
            GROUP_ID);
        Assert.assertNotNull(listenerContainer);
        Assert.assertFalse(listenerContainer.isRunning());
        // except container started and new consumer added and poll records started
        receiveConsumerStartedEvent = false;
        listenerContainer.start();
        while (!receiveConsumerStartedEvent) {
            TimeUnit.SECONDS.sleep(1);
        }
        Assert.assertEquals(1, CONSUMER_COUNTER.get());
        Assert.assertTrue(listenerContainer.isRunning());
        String recordValue = "testConsumerStart";
        long oldOffset = lastRecord.offset();
        kafkaTemplate.send(DEFAULT_TOPIC, recordValue);
        awaitForNextPoll(listenerContainer);
        Assert.assertEquals(recordValue, lastRecord.value());
        Assert.assertNotEquals(oldOffset, lastRecord.offset());
    }

    private void testStartPauseRequestedContainerAndExceptConsumerPollRecordsPaused()
        throws InterruptedException {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(
            GROUP_ID);
        Assert.assertNotNull(listenerContainer);
        if (listenerContainer.isRunning()) {
            receiveConsumerStoppedEvent = false;
            listenerContainer.stop();
            while (!receiveConsumerStoppedEvent) {
                TimeUnit.SECONDS.sleep(1);
            }
            Assert.assertEquals(0, CONSUMER_COUNTER.get());
            Assert.assertFalse(listenerContainer.isRunning());
        }
        // ensure stopped container pause requested
        listenerContainer.pause();
        Assert.assertTrue(listenerContainer.isPauseRequested());
        receiveConsumerStartedEvent = false;
        listenerContainer.start();
        while (!receiveConsumerStartedEvent) {
            TimeUnit.SECONDS.sleep(1);
        }
        Assert.assertEquals(1, CONSUMER_COUNTER.get());
        Assert.assertTrue(listenerContainer.isRunning());
        Assert.assertTrue(listenerContainer.isPauseRequested());
        String recordValue = "testStartPauseRequestedContainerAndExceptConsumerPollRecordsPaused";
        long oldOffset = lastRecord.offset();
        kafkaTemplate.send(DEFAULT_TOPIC, recordValue);
        awaitForNextPoll(listenerContainer);
        // except container paused and consumer poll records paused
        Assert.assertTrue(listenerContainer.isContainerPaused());
        Assert.assertNotEquals(recordValue, lastRecord.value());
        Assert.assertEquals(oldOffset, lastRecord.offset());
    }

    private void awaitForNextPoll(MessageListenerContainer messageListenerContainer) {
        try {
            long awaitMillis = Math.max(DEFAULT_MAX_POLL_INTERVAL_MS + 200,
                messageListenerContainer.getContainerProperties()
                    .getIdleBetweenPolls());
            TimeUnit.MILLISECONDS.sleep(awaitMillis);
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    @TestConfiguration
    public static class KafkaTestConfig {

        @Resource
        private EmbeddedKafkaBroker embeddedKafkaBroker;

        @Bean
        public ProducerFactory<Integer, String> producerFactory() {
            return new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(embeddedKafkaBroker));
        }

        @Bean
        public KafkaTemplate<Integer, String> kafkaTemplate() {
            KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
            kafkaTemplate.setDefaultTopic(DEFAULT_TOPIC);
            return kafkaTemplate;
        }

        @Bean
        public ConsumerFactory<Integer, String> consumerFactory() {
            Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(GROUP_ID,
                Boolean.TRUE.toString(),
                embeddedKafkaBroker);
            consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                DEFAULT_MAX_POLL_INTERVAL_MS);
            DefaultKafkaConsumerFactory<Integer, String> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(
                consumerProps);
            kafkaConsumerFactory.addListener(consumerListener());
            return kafkaConsumerFactory;
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
            kafkaListenerContainerFactory.setConsumerFactory(consumerFactory());
            kafkaListenerContainerFactory.setAutoStartup(false);
            return kafkaListenerContainerFactory;
        }

        @Bean
        public ConsumerListener consumerListener() {
            return new ConsumerListener();
        }

    }

    public static class ConsumerListener implements ConsumerFactory.Listener<Integer, String>,
        ApplicationListener<KafkaEvent> {

        @Override
        public void consumerAdded(String id, Consumer<Integer, String> consumer) {
            CONSUMER_COUNTER.incrementAndGet();
        }

        @Override
        public void consumerRemoved(String id, Consumer<Integer, String> consumer) {
            CONSUMER_COUNTER.decrementAndGet();
        }

        @Override
        public void onApplicationEvent(KafkaEvent event) {
            if (event instanceof ConsumerPausedEvent) {
                receiveConsumerPausedEvent = true;
            } else if (event instanceof ConsumerResumedEvent) {
                receiveConsumerResumedEvent = true;
            } else if (event instanceof ConsumerStoppedEvent) {
                receiveConsumerStoppedEvent = true;
            } else if (event instanceof ConsumerStartedEvent) {
                receiveConsumerStartedEvent = true;
            }
            System.out.println("receive KafkaEvent: " + event);
        }
    }
}
