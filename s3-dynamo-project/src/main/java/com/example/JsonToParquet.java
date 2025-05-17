package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.hadoop.fs.Path;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;

public class JsonToParquet {
    public static void convert(String jsonFilePath, String parquetFilePath) throws IOException {
        String schemaStr = """
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"}
                ]
            }""";
        
        Schema schema = new Schema.Parser().parse(schemaStr);
        
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(parquetFilePath))
                .withSchema(schema)
                .build()) {
            
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonArray = mapper.readTree(new File(jsonFilePath));
            
            for (JsonNode node : jsonArray) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", node.get("id").asText());
                record.put("name", node.get("name").asText());
                record.put("age", node.get("age").asInt());
                writer.write(record);
            }
        }
    }
}