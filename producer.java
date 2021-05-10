import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
public class producer{
	final static Logger logger = LoggerFactory.getLogger(StudentBean.class);
    public static void main(String[] args) throws Exception{	      
    // Check arguments length value
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }
      
      //Assign topicName to string variable
      String topicName = args[0].toString();
      
      // create instance for properties to access producer configs   
      Properties props = new Properties();
      props.put("producer.type", "async");
      props.put("bootstrap.servers", "localhost:9092");
      //props.put("metadata.broker.list", "10.43.100.131:9092");
      props.put("acks", "0");
      props.put("retries", 1);            
      props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");         
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
      Producer<String, String> producer = new KafkaProducer
         <String, String>(props);
      
      ProducerRecord<String, StudentBean> record = 
    		  new ProducerRecord<String, StudentBean>("first_topic", "message from java" + Integer.toString(0));
      producer.send(record, new Callback() {
           public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        	   if (e == null) {
                    // the record is sent successfully        		   
                    logger.debug("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }

           }
      });

      /*System.out.println("Start");
      for(int i = 0; i < 10; i++) {
          producer.send(new ProducerRecord<StudentBean>(topicName, 
             Integer.toString(i), Integer.toString(i)));  
          System.out.println("send"+i);
      }*/
      
      System.out.println("Message sent successfully");
      producer.close();
   }
}
