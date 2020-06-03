package com.puneetbabbar.kafka.streams.example;

import com.sun.corba.se.spi.presentation.rmi.IDLNameTranslator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class FavColorApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favColorApp");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Consumer property
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> userpPrefTable = builder.table("user-fav-color-input");

//        KTable<String, String> transform = userpPrefTable.filterNot((key,value) -> value.equalsIgnoreCase("green") );
//((key,value) -> value == "green" || value == "red"  || value == "blue"  );

        KTable<String, Long> colorCount = userpPrefTable.toStream().selectKey((key, word) -> word).groupByKey().count();

        colorCount.toStream().foreach((key, value) -> System.out.println(key + " => " + value));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

    }

}
