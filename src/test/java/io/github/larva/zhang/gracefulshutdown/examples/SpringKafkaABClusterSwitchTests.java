package io.github.larva.zhang.gracefulshutdown.examples;

import io.github.larva.zhang.gracefulshutdown.examples.SpringKafkaABClusterSwitchTests.KafkaABClusterSwitchTestConfig;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ABSwitchCluster;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * SpringKafkaABClusterSwitchTests
 *
 * @author zhanghan
 * @date 2022/7/30
 * @since 1.0
 */
@EnableKafka
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = KafkaABClusterSwitchTestConfig.class)
public class SpringKafkaABClusterSwitchTests {

    private static final String DEFAULT_TOPIC = "templateTopic";

    private static final String GROUP_ID = "test-consumer-group";

    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 500;

    private static final int CLUSTER_A_PORT = 3333;
    private static final String CLUSTER_A_BOOTSTRAP_SERVERS = "127.0.0.1:" + CLUSTER_A_PORT;
    private static final int CLUSTER_B_PORT = 4444;
    private static final String CLUSTER_B_BOOTSTRAP_SERVERS = "127.0.0.1:" + CLUSTER_B_PORT;

    @ClassRule
    public static EmbeddedKafkaRule kafkaRuleA = new EmbeddedKafkaRule(1, true, 1,
        DEFAULT_TOPIC).kafkaPorts(CLUSTER_A_PORT);

    @ClassRule
    public static EmbeddedKafkaRule kafkaRuleB = new EmbeddedKafkaRule(1, true, 1,
        DEFAULT_TOPIC).kafkaPorts(CLUSTER_B_PORT);
    private static volatile boolean receiveConsumerStoppedEvent;
    private static volatile boolean receiveConsumerStartedEvent;
    private volatile ConsumerRecord<Integer, String> lastRecord = new ConsumerRecord<>(
        DEFAULT_TOPIC, 1, -1, -1, "init record");
    @Resource
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Resource
    private KafkaTemplate<Integer, String> kafkaTemplateA;

    @Resource
    private KafkaTemplate<Integer, String> kafkaTemplateB;

    @Resource
    private ABSwitchCluster abSwitchCluster;

    @KafkaListener(id = GROUP_ID, topics = DEFAULT_TOPIC)
    public void KafkaListener(ConsumerRecord<Integer, String> consumerRecord) {
        lastRecord = consumerRecord;
    }

    @Test
    public void testABClusterSwitch() throws InterruptedException {
        while (!receiveConsumerStartedEvent) {
            TimeUnit.SECONDS.sleep(1);
        }
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(
            GROUP_ID);
        Assert.assertNotNull(listenerContainer);
        Assert.assertTrue(abSwitchCluster.isPrimary());
        kafkaTemplateA.send(DEFAULT_TOPIC, "testClusterA");
        awaitForNextPoll(listenerContainer);
        Assert.assertEquals(0, lastRecord.offset());
        Assert.assertEquals("testClusterA", lastRecord.value());
        receiveConsumerStoppedEvent = false;
        listenerContainer.stop();
        while (!receiveConsumerStoppedEvent) {
            TimeUnit.SECONDS.sleep(1);
        }
        // switch cluster b
        abSwitchCluster.secondary();
        Assert.assertFalse(abSwitchCluster.isPrimary());
        receiveConsumerStartedEvent = false;
        listenerContainer.start();
        while (!receiveConsumerStartedEvent) {
            TimeUnit.SECONDS.sleep(1);
        }
        Assert.assertTrue(listenerContainer.isRunning());
        kafkaTemplateB.send(DEFAULT_TOPIC, "testClusterB");
        awaitForNextPoll(listenerContainer);
        Assert.assertEquals(0, lastRecord.offset());
        Assert.assertEquals("testClusterB", lastRecord.value());
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
    public static class KafkaABClusterSwitchTestConfig {

        @Bean
        public KafkaTemplate<Integer, String> kafkaTemplateA() {
            DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(kafkaRuleA.getEmbeddedKafka()));
            KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
            kafkaTemplate.setDefaultTopic(DEFAULT_TOPIC);
            return kafkaTemplate;
        }

        @Bean
        public KafkaTemplate<Integer, String> kafkaTemplateB() {
            DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(kafkaRuleB.getEmbeddedKafka()));
            KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
            kafkaTemplate.setDefaultTopic(DEFAULT_TOPIC);
            return kafkaTemplate;
        }

        @Bean
        public ABSwitchCluster abSwitchCluster() {
            return new ABSwitchCluster(CLUSTER_A_BOOTSTRAP_SERVERS, CLUSTER_B_BOOTSTRAP_SERVERS);
        }

        @Bean
        public ConsumerFactory<Integer, String> consumerFactory() {
            Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(GROUP_ID,
                Boolean.TRUE.toString(),
                kafkaRuleA.getEmbeddedKafka());
            consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                DEFAULT_MAX_POLL_INTERVAL_MS);
            DefaultKafkaConsumerFactory<Integer, String> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(
                consumerProps);
            kafkaConsumerFactory.setBootstrapServersSupplier(abSwitchCluster());
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

    public static class ConsumerListener implements ApplicationListener<KafkaEvent> {

        @Override
        public void onApplicationEvent(KafkaEvent event) {
            if (event instanceof ConsumerStoppedEvent) {
                receiveConsumerStoppedEvent = true;
            } else if (event instanceof ConsumerStartedEvent) {
                receiveConsumerStartedEvent = true;
            }
            System.out.println("receive KafkaEvent: " + event);
        }
    }
}
