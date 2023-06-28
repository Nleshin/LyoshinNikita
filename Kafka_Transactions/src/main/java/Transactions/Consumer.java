package Transactions;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class Consumer {

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "test" + "-" + UUID.randomUUID());
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        var consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList(Utils.topics));

        System.out.println("Чтение пачки сообщений:");
        for (int i = 0; i < 100; i++) {
            System.out.println("*");
            }
        try
        {

            while(true) {
                var result = consumer.poll(Duration.ofSeconds(10));

                for (var record : result) {
                    System.out.println("Сообщение: ключ-" + record.key() + " содержимое-" + record.value() + " название топика-" + record.topic());
                }
            }
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            consumer.close();
        }

    }


    }
