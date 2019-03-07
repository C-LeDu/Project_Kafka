package eu.nyuu.courses.rest;

import eu.nyuu.courses.model.CountFeeling;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

@RestController
public class Controller {
    private final KafkaStreams streams;

    public Controller() {
        final String bootstrapServers = "51.15.90.153:9092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "larcher-ledu-gauriat-api-stream-app");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "larcher-ledu-gauriat-api-stream-app-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\Chaah\\Documents\\tmp");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        this.streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @GetMapping("/day")
    public CountFeeling blablabla(){

        ReadOnlyWindowStore<String, CountFeeling> windowStore = streams.store("larcher_ledu_gauriat_user_distribution_feeling", QueryableStoreTypes.windowStore());
        Instant now = Instant.now();
        Instant lastMinute = now.minus(Duration.ofMinutes(1));

        KeyValueIterator<Windowed<String>, CountFeeling> iterator = windowStore.fetchAll(lastMinute, now);
        return iterator.next().value;
    }


}
