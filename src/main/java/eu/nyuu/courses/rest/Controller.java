package eu.nyuu.courses.rest;

import edu.stanford.nlp.simple.Sentence;
import eu.nyuu.courses.model.CountFeeling;
import eu.nyuu.courses.model.FeelingEvent;
import eu.nyuu.courses.model.TwitterEvent;
import eu.nyuu.courses.serdes.SerdeFactory;
import javafx.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.logging.log4j.util.Strings;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "F:\\Kafka\\tmp");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        Serde<TwitterEvent> twitterEventSerde = SerdeFactory.createSerde(TwitterEvent.class, serdeProps);
        Serde<FeelingEvent> feelingEventSerde = SerdeFactory.createSerde(FeelingEvent.class, serdeProps);
        Serde<CountFeeling> countFeelingSerde = SerdeFactory.createSerde(CountFeeling.class, serdeProps);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, TwitterEvent> twittersStream = builder
                .stream("tweets", Consumed.with(stringSerde, twitterEventSerde));

        KStream<String, FeelingEvent> stringFeelingEventKStream = twittersStream
                .peek((s, feelingEvent) -> System.out.println(feelingEvent))
                .mapValues((s, twitterEvent) -> {
                    String feeling = new Sentence(twitterEvent.getBody()).sentiment().toString();
                    return new FeelingEvent(twitterEvent, feeling);
                });

        // PUSH COUNT FEELING BY USER
        stringFeelingEventKStream
                .map((s, feelingEvent) ->  KeyValue.pair(feelingEvent.getNick(), feelingEvent))
                .groupByKey(Grouped.with(stringSerde, feelingEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10L)))
                .aggregate(CountFeeling::new,
                        (s, feelingEvent, countFeeling) -> {
                            addFeeling(feelingEvent, countFeeling);
                            return countFeeling;
                        }, Materialized.<String, CountFeeling, WindowStore<Bytes, byte[]>>as("larcher_ledu_gauriat_user_distribution_feeling").withValueSerde(countFeelingSerde).withKeySerde(stringSerde))
                .toStream().print(Printed.toSysOut());


        // PUSH COUNT FEELING BY USER BY DAY
        stringFeelingEventKStream
                .map((s, feelingEvent) ->  KeyValue.pair(feelingEvent.getNick(), feelingEvent))
                .groupByKey(Grouped.with(stringSerde, feelingEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                .aggregate(CountFeeling::new,
                        (s, feelingEvent, countFeeling) -> {
                            addFeeling(feelingEvent, countFeeling);
                            return countFeeling;
                        }, Materialized.<String, CountFeeling, WindowStore<Bytes, byte[]>>as("larcher_ledu_gauriat_day_distribution_feeling").withValueSerde(countFeelingSerde).withKeySerde(stringSerde))
                ;

        // PUSH COUNT FEELING BY USER BY MONTH
        stringFeelingEventKStream
                .map((s, feelingEvent) ->  KeyValue.pair(feelingEvent.getNick(), feelingEvent))
                .groupByKey(Grouped.with(stringSerde, feelingEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .aggregate(CountFeeling::new,
                        (s, feelingEvent, countFeeling) -> {
                            addFeeling(feelingEvent, countFeeling);
                            return countFeeling;
                        }, Materialized.<String, CountFeeling, WindowStore<Bytes, byte[]>>as("larcher_ledu_gauriat_month_distribution_feeling").withValueSerde(countFeelingSerde).withKeySerde(stringSerde));

        // PUSH COUNT FEELING BY USER BY YEAR
        stringFeelingEventKStream
                .map((s, feelingEvent) ->  KeyValue.pair(feelingEvent.getNick(), feelingEvent))
                .groupByKey(Grouped.with(stringSerde, feelingEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(365)))
                .aggregate(CountFeeling::new,
                        (s, feelingEvent, countFeeling) -> {
                            addFeeling(feelingEvent, countFeeling);
                            return countFeeling;
                        }, Materialized.<String, CountFeeling, WindowStore<Bytes, byte[]>>as("larcher_ledu_gauriat_year_distribution_feeling").withValueSerde(countFeelingSerde).withKeySerde(stringSerde));

        KStream<String, TwitterEvent> hashtwitterEventKStream = twittersStream
                .flatMap((s, twitterEvent) -> {
                    List<KeyValue<String, TwitterEvent>> result = new LinkedList<>();
                    Matcher m = Pattern.compile("(?:\\s|\\A)[#]+[A-Za-z0-9-_]+")
                            .matcher(twitterEvent.getBody());
                    while (m.find()) {
                        result.add(new KeyValue<String, TwitterEvent>(m.group(), twitterEvent));
                    }
                    return result;
                });

        hashtwitterEventKStream
                .groupByKey(Grouped.with(stringSerde, twitterEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("larcher_ledu_gauriat_day_count_hash"));

        hashtwitterEventKStream
                .groupByKey(Grouped.with(stringSerde, twitterEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("larcher_ledu_gauriat_month_count_hash"));

        this.streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @GetMapping("/user")
    public CountFeeling getUser(@RequestParam(name = "date", defaultValue = "") @DateTimeFormat(pattern="dd-MM-yyyy") LocalDate date, @RequestParam(name = "name") String name){
        if (streams.state() != KafkaStreams.State.RUNNING)
            return null;
        ReadOnlyWindowStore<String, CountFeeling> windowStore = streams.store("larcher_ledu_gauriat_day_distribution_feeling", QueryableStoreTypes.windowStore());
        Instant start = date == null ? LocalDateTime.now().toInstant(ZoneOffset.UTC): date.plusDays(1L).atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant end = start.minus(Duration.ofDays(1));

        return getCountFeeling(windowStore, start, end, name);
    }

    @GetMapping("/day")
    public CountFeeling getDay(@RequestParam(name = "date", defaultValue = "") @DateTimeFormat(pattern="dd-MM-yyyy") LocalDate date){
        if (streams.state() != KafkaStreams.State.RUNNING)
            return null;
        ReadOnlyWindowStore<String, CountFeeling> windowStore = streams.store("larcher_ledu_gauriat_day_distribution_feeling", QueryableStoreTypes.windowStore());
        Instant start = date == null ? LocalDateTime.now().toInstant(ZoneOffset.UTC): date.plusDays(1L).atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant end = start.minus(Duration.ofDays(1));

        return getCountFeeling(windowStore, start, end);
    }

    @GetMapping("/month")
    public CountFeeling getMonth(@RequestParam(name = "date", defaultValue = "03-2019") String date){
        if (streams.state() != KafkaStreams.State.RUNNING)
            return null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM-yyyy");
        YearMonth ym = YearMonth.parse(date, formatter);
        ReadOnlyWindowStore<String, CountFeeling> windowStore = streams.store("larcher_ledu_gauriat_month_distribution_feeling", QueryableStoreTypes.windowStore());
        Instant end = Strings.isBlank(date)? Instant.now() : ym.atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant start = end.plus(Duration.ofDays(30));

        return getCountFeeling(windowStore, start, end);
    }

    @GetMapping("/year")
    public CountFeeling getYear(@RequestParam(name = "date", defaultValue = "2019") String date){
        if (streams.state() != KafkaStreams.State.RUNNING)
            return null;
        if(Strings.isNotBlank(date))
            date = "01-" + date;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM-yyyy");
        YearMonth ym = YearMonth.parse(date, formatter);
        ReadOnlyWindowStore<String, CountFeeling> windowStore = streams.store("larcher_ledu_gauriat_month_distribution_feeling", QueryableStoreTypes.windowStore());
        Instant end = Strings.isBlank(date)? Instant.now() : ym.atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant start = end.plus(Duration.ofDays(365));

        return getCountFeeling(windowStore, start, end);
    }

    @GetMapping("/Dhashtag")
    public List<Pair<String, Long>> getDBestHashtag(@RequestParam(name = "number", defaultValue = "1") int number ){
        if (streams.state() != KafkaStreams.State.RUNNING)
            return null;
        ReadOnlyWindowStore<String, Long> windowStore = streams.store("larcher_ledu_gauriat_day_count_hash", QueryableStoreTypes.windowStore());
        Instant start = Instant.now();
        Instant end = start.minus(Duration.ofDays(1));

        return getPairs(windowStore, start, end).stream()
                .sorted((f1, f2) -> Long.compare(f2.getValue(), f1.getValue()))
                .limit(number)
                .collect(Collectors.toList());
    }

    @GetMapping("/Mhashtag")
    public List<Pair<String, Long>> getMBestHashtag(@RequestParam(name = "number", defaultValue = "1") int number ){
        if (streams.state() != KafkaStreams.State.RUNNING)
            return null;
        ReadOnlyWindowStore<String, Long> windowStore = streams.store("larcher_ledu_gauriat_month_count_hash", QueryableStoreTypes.windowStore());
        Instant start = Instant.now();
        Instant end = start.minus(Duration.ofDays(30));

        return getPairs(windowStore, start, end).stream()
                .sorted((f1, f2) -> Long.compare(f2.getValue(), f1.getValue()))
                .limit(number)
                .collect(Collectors.toList());
    }

    private List<Pair<String, Long>> getPairs(ReadOnlyWindowStore<String, Long> windowStore, Instant start, Instant end) {
        KeyValueIterator<Windowed<String>, Long> iterator = windowStore.fetchAll(end, start);
        List<Pair<String, Long>> res = new ArrayList<>();
        while (iterator.hasNext()) {
            KeyValue<Windowed<String>, Long> next = iterator.next();
            res.add(new Pair<>(next.key.key(),next.value));
        }
        return res;
    }

    private CountFeeling getCountFeeling(ReadOnlyWindowStore<String, CountFeeling> windowStore, Instant startDuration, Instant endDuration) {
        KeyValueIterator<Windowed<String>, CountFeeling> iterator = windowStore.fetchAll(endDuration, startDuration);
        CountFeeling result = new CountFeeling();
        while (iterator.hasNext()) {
            KeyValue<Windowed<String>, CountFeeling> next = iterator.next();
            result.addPosiftif(next.value.getPositif());
            result.addNegatif(next.value.getNegatif());
            result.addNeutre(next.value.getNeutre());
        }
        return result;
    }

    private CountFeeling getCountFeeling(ReadOnlyWindowStore<String, CountFeeling> windowStore, Instant startDuration, Instant endDuration, String name) {
        KeyValueIterator<Windowed<String>, CountFeeling> iterator = windowStore.fetchAll(endDuration, startDuration);
        CountFeeling result = new CountFeeling();
        while (iterator.hasNext()) {
            KeyValue<Windowed<String>, CountFeeling> next = iterator.next();
            if (next.key.equals(name)) {
                result.addPosiftif(next.value.getPositif());
                result.addNegatif(next.value.getNegatif());
                result.addNeutre(next.value.getNeutre());

            }
        }
        return result;
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

}
