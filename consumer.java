import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumer {
    private static String bootstrapServers = "server:9092";
    private static String groupId = "my-forth-app";
    private static String topic = "first_topic";


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumer.class);

        /**
         * create Consumer properties
         *
         * Properties are available in official document:
         * https://kafka.apache.org/documentation/#consumerconfigs
         *
         */
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMinutes(100));

            for(ConsumerRecord record : records){
                logger.info("Key: " + record.key() + "\t" + "Value: " + record.value() +
                        "Topic: " + record.partition() + "\t" + "Partition: " + record.partition()
               );

            }
        }

    }
}