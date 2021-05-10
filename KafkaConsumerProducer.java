public class KafkaConsumerProducer
{
    public static void main(String[] args)
    {
        producer producerThread = new producer(KafkaProperties.topic);
        producerThread.start();
        consumer consumerThread = new consumer(KafkaProperties.topic);
        consumerThread.start();
    }
}
