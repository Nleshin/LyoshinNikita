package Transactions;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) throws Exception {
        Utils.delete_topics(Utils.topics);

        Properties prop = new Properties();

          prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
          prop.put(ProducerConfig.ACKS_CONFIG, "all");
          prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
          prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
          prop.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test_transactions");

          try(
            var producer = new KafkaProducer<String, String>(prop);
          )
          {
              producer.initTransactions();
              send_messages(producer, 5, true);
              send_messages(producer, 2, false);
          }
          catch(Exception e)
          {
              throw new Exception("Ошибка при отправке!");
          }


    }

    public static void send_messages(KafkaProducer<String, String> producer, Integer count_messages, Boolean commit_transaction) {

        Faker faker = new Faker();
        for (String topic : Utils.topics) {
            producer.beginTransaction();
            for (int j = 0; j < count_messages; j++) {
                String key = String.valueOf(faker.idNumber().hashCode());
                String name = faker.name().name();
                System.out.println("Сообщение: ключ-" + key + " значение-" + name + "!");
                producer.send(new ProducerRecord<>(topic, key, name));
            }
            producer.flush();
            if (commit_transaction) {
                System.out.println("Отправлено " + count_messages + " сообщений в топик " + topic + "! Транзакция подтвержена!");
                producer.commitTransaction();
            } else {
                System.out.println("Отправлено " + count_messages + " сообщений в топик " + topic + "! Транзакция отменена!");
                producer.abortTransaction();
            }
        }
    }
}
