package Streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class Streams {
    public static void main(String[] args){

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream_count = builder
                .stream("events", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)))
                .count()
                .toStream().map((k, v) -> new KeyValue<>("Event key: " + k.key(), "Last count events by key " + k.key() + " : " + v));

        stream_count.to("events_group_by_key", Produced.with(Serdes.String(), Serdes.String()));

        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_config_test");

        System.out.println(builder.build().describe());
        try (
                var kafkaStreams = new KafkaStreams(builder.build(), prop);
        ) {
            kafkaStreams.start();
            while ((true))
            {}
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

}

