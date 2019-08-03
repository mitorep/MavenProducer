package com.mycompany.app;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        System.setProperty("java.security.auth.login.config", "/provision/projects/Kafka/java_jaas.conf");

        final String TOPIC_NAME = "testowy1";
        final Logger LOGGER = Logger.getLogger(Producer.class.getName());

        LOGGER.log(Level.INFO, "Kafka Producer running in thread {0}", Thread.currentThread().getName());
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1.mito.local:9092, kafka2.mito.local:9092, kafka3.mito.local:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "0");
        kafkaProps.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 1);
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.put("sasl.kerberos.service.name", "kafka");

        KafkaProducer<String, String> kafkaProducer  = new KafkaProducer<>(kafkaProps);


        ProducerRecord<String, String> record = null;
        try {
            Random rnd = new Random();
            while (true) {

                for (int i = 1; i <= 10; i++) {
                    String key = "machine-" + i;
                    String value = "message" + rnd.nextInt(20); //-- Set value automaticaly

                    //Scanner scan = new Scanner(System.in);  //Set value from console
                    //String value = scan.nextLine();

                    record = new ProducerRecord<>(TOPIC_NAME, key, value);

                    kafkaProducer.send(record);
                    /**
                     * wait before sending next message. this has been done on
                     * purpose
                     */
                    Thread.sleep(1000);
                }

            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Producer thread was interrupted");
        } finally {
            kafkaProducer.close();

            LOGGER.log(Level.INFO, "Producer closed");
        }


    }
}
