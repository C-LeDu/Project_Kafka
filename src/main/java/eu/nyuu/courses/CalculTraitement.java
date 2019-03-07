package eu.nyuu.courses;

import edu.stanford.nlp.simple.Sentence;
import eu.nyuu.courses.model.CountFeeling;
import eu.nyuu.courses.model.FeelingEvent;
import eu.nyuu.courses.model.TweetEvent;
import eu.nyuu.courses.serdes.SerdeFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CalculTraitement {

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "51.15.90.153:9092";
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "larcher-ledu-gauriat-stream-app");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "larcher-ledu-gauriat-stream-app-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\Chaah\\Documents\\tmp");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<TweetEvent> tweetEventSerde = SerdeFactory.createSerde(TweetEvent.class, serdeProps);
        final Serde<FeelingEvent> feelingEventSerde = SerdeFactory.createSerde(FeelingEvent.class, serdeProps);

        // Stream
        final StreamsBuilder builder = new StreamsBuilder();

        // Here you go :)

        final KStream<String, TweetEvent> tweetsStream = builder
                .stream("tweets", Consumed.with(stringSerde, tweetEventSerde));

        KStream<String, FeelingEvent> stringFeelingEventKStream = tweetsStream
                .peek((s, feelingEvent) -> System.out.println(feelingEvent))
                .mapValues((s, tweetEvent) -> {
                    String feeling = new Sentence(tweetEvent.getBody()).sentiment().toString();
                    return new FeelingEvent(tweetEvent, feeling);
                });

        // PUSH COUNT FEELING BY USER
        stringFeelingEventKStream
                .map((s, feelingEvent) ->  KeyValue.pair(feelingEvent.getNick(), feelingEvent))
                .groupByKey(Grouped.with(stringSerde, feelingEventSerde))
                .windowedBy(SessionWindows.with(Duration.ofMinutes(5L)))
                .aggregate(CountFeeling::new,
                        (s, feelingEvent, countFeeling) -> {
                            addFeeling(feelingEvent, countFeeling);
                            return countFeeling;
                }, (s, countFeeling, v1) -> {
                                CountFeeling newCountFeeling = new CountFeeling();
                                newCountFeeling.setNegatif(countFeeling.getNegatif() + v1.getNegatif());
                                newCountFeeling.setNeutre(countFeeling.getNeutre() + v1.getNeutre());
                                newCountFeeling.setPositif(countFeeling.getPositif() + v1.getPositif());
                                return newCountFeeling;
                        }, Materialized.as("larcher_ledu_gauriat_user_distribution_feeling"));


        // PUSH COUNT FEELING BY USER BY DAY
        stringFeelingEventKStream
                .map((s, feelingEvent) ->  KeyValue.pair(feelingEvent.getNick(), feelingEvent))
                .groupByKey(Grouped.with(stringSerde, feelingEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                .aggregate(CountFeeling::new,
                        (s, feelingEvent, countFeeling) -> {
                            addFeeling(feelingEvent, countFeeling);
                            return countFeeling;
                        }, Materialized.as("larcher_ledu_gauriat_day_distribution_feeling"));

        // PUSH COUNT FEELING BY USER BY MONTH
        stringFeelingEventKStream
                .map((s, feelingEvent) ->  KeyValue.pair(feelingEvent.getNick(), feelingEvent))
                .groupByKey(Grouped.with(stringSerde, feelingEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .aggregate(CountFeeling::new,
                        (s, feelingEvent, countFeeling) -> {
                            addFeeling(feelingEvent, countFeeling);
                            return countFeeling;
                        }, Materialized.as("larcher_ledu_gauriat_month_distribution_feeling"));

        // PUSH COUNT FEELING BY USER BY YEAR
        stringFeelingEventKStream
                .map((s, feelingEvent) ->  KeyValue.pair(feelingEvent.getNick(), feelingEvent))
                .groupByKey(Grouped.with(stringSerde, feelingEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(365)))
                .aggregate(CountFeeling::new,
                        (s, feelingEvent, countFeeling) -> {
                            addFeeling(feelingEvent, countFeeling);
                            return countFeeling;
                        }, Materialized.as("larcher_ledu_gauriat_year_distribution_feeling"));

        KStream<String, TweetEvent> hashTweetEventKStream = tweetsStream
                .flatMap((s, tweetEvent) -> {
                    List<KeyValue<String, TweetEvent>> result = new LinkedList<>();
                    Matcher m = Pattern.compile("(?:\\s|\\A)[##]+[A-Za-z0-9-_]+")
                            .matcher(tweetEvent.getBody());
                    while (m.find()) {
                        result.add(new KeyValue<String, TweetEvent>(m.group(), tweetEvent));
                    }
                    return result;
                });

        hashTweetEventKStream
                .groupByKey(Grouped.with(stringSerde, tweetEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                .count(Materialized.as("larcher_ledu_gauriat_day_count_hash"));

        hashTweetEventKStream
                .groupByKey(Grouped.with(stringSerde, tweetEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .count(Materialized.as("larcher_ledu_gauriat_month_count_hash"));


        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

    private static void addFeeling(FeelingEvent feelingEvent, CountFeeling countFeeling) {
        switch (feelingEvent.getFeeling()) {
            case "NEGATIVE":
            case "VERY_NEGATIVE":
                countFeeling.addNegatif();
                break;
            case "POSITIVE":
            case "VERY_POSITIVE":
                countFeeling.addPosiftif();
                break;
            case "NEUTRAL":
                countFeeling.addNeutre();
                break;
            default:
                break;
        }
    }
};
