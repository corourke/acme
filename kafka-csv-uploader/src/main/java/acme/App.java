package acme;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.validation.Schema;

public class App {
    static SpecificRecord schema;
    static Map<String, Integer> fieldNameToIndex = new HashMap<>();

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: kafka-csv-producer <topic_name> <data_file.csv> <avro_schema_file.avsc>");
            System.exit(1);
        }

        String topic = args[0];
        String dataFile = args[1];
        String schemaFile = args[2];

        // Schema pre-processing
        schema = getSchemaFromFile(schemaFile); // Read the schema once
        prepareFieldMapping(); // Create the mapping of field names to indexes

        Properties props = loadConfig("config.properties");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(new FileReader(datafile));

            for (CSVRecord csvRecord : parser) {
                GenericRecord avroRecord = createAvroRecord(csvRecord); // Implement this method
                producer.send(new ProducerRecord<>(topic, null, avroRecord));
            }
        }
    }

    private static Properties loadConfig(String configFile) throws IOException {
        Properties props = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            props.load(inputStream);
        }
        return props;
    }

    private static void prepareFieldMapping() {
        for (int i = 0; i < schema.schema().getFields().size(); i++) {
            fieldNameToIndex.put(schema.schema().getFields().get(i).name(), i);
        }
    }

    private static GenericRecord createAvroRecord(CSVRecord csvRecord) throws IOException {
        String fieldName;
        Object fieldValue;

        // Use a HashMap to temporarily store CSV data by field name
        Map<String, Object> csvData = new HashMap<>();
        for (String header : csvRecord.getHeaderNames()) {
            fieldName = header;
            fieldValue = csvRecord.get(header);
            csvData.put(fieldName, fieldValue);
        }

        // Build the Avro record using reflection
        GenericRecord.Builder builder = new GenericRecord.Builder(schema);
        for (String csvFieldName : csvData.keySet()) {
            // Look up the field index
            int index = fieldNameToIndex.getOrDefault(csvFieldName, -1);
            if (index != -1) {
                builder.put(index, csvData.get(csvFieldName));
            } else {
                // Handle missing fields
                console.error("Field " + csvFieldName + " not found in CSV data");
            }
        }
        return builder.build();
    }

    // Helper method to get the SpecificRecord from the Avro schema file
    private static SpecificRecord getSchemaFromFile(String schemaFile) throws IOException {
        Schema schema = new SchemaParser().parse(new File(schemaFile));
        return SpecificRecord.newRecord(schema, null);
    }

    // Helper method for data type conversion (example for String to
    // Integer)
    private static Object convertIfNecessary(Object value, Schema fieldSchema) {
        if (fieldSchema.getType() == Schema.Type.INT) {
            if (value instanceof String) {
                try {
                    return Integer.parseInt((String) value);
                } catch (NumberFormatException e) {
                    // Handle parsing exception (log error, set default value)
                }
            }
        }
        return value; // No conversion if types match or unsupported conversion
    }

}
