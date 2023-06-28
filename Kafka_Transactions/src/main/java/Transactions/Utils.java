package Transactions;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

public class Utils {
    static String[] topics = {"topic1", "topic2"};

    public static void delete_topics(String[] topics) {
        Utils.topics = topics;
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");

        var admin = Admin.create(prop);
        admin.deleteTopics(Arrays.asList(topics));
    }
}
