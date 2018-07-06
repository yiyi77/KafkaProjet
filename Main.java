package eu.nyuu.courses;

import eu.nyuu.courses.model.SensorEvent;
import eu.nyuu.courses.serdes.SerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class Main {

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:29092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-app");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "my-stream-app-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<SensorEvent> sensorEventSerde = SerdeFactory.createSerde(SensorEvent.class, serdeProps);

        // Stream
        final StreamsBuilder builder = new StreamsBuilder();
        
        final KStream<byte[], String> textLines = builder.stream("raw_tweets", Consumed.with(byteArraySerde, stringSerde));
        
        _.MapValues(Value -> {
            JSONParser parser = new JSONParser();
            Object jsonObj = null;
            
            (ev -> {
                try {
                    System.out.println(mapper.writeValueAsString(ev));
                    ev.setSource(sourceApi.fetchSourceName(ev.getSource()));
                } catch (JsonProcessingException ex) {
                    System.out.println("Error writing ev");
                    return ev;
                }
                return ev;
            })
            .mapValues(ev -> {
                try {
                    System.out.println(mapper.writeValueAsString(ev));
                } catch (JsonProcessingException ex) {
                    System.out.println("Error writing ev");
                    return ev;
                }
                return ev;
            });    
            
            
        
        // val map = mapOf("date" to _ , "requete" to _ , "nom" to _ , "tweet" to _ );
        //val mapV = map.mapValues { it.value.toString()};
        //println(mapV) 
            
            
        // Here you go :)

        final KStream<String, SensorEvent> sensorsStream = builder
        		.stream("raw_tweets");

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
   }
};
