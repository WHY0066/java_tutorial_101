package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetFileWriter;

import java.io.File;


import org.apache.hadoop.fs.Path;

public class JsonToParquetConverter {
    public void convertJsonToParquet(String jsonPath, String parquetPath, Schema avroSchema) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(jsonPath));

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(parquetPath))
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {

            for (JsonNode node : root) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("id", node.get("id").asText());
                record.put("name", node.get("name").asText());
                record.put("age", node.get("age").asInt());
                writer.write(record);
            }
        }
    }

    public Schema getSchema() {
        return new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
              ]
            }
        """);
    }
}
