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
        // 这里用一个简单的示例 Avro schema，按需替换
        String schemaStr = "{"
                + "\"type\":\"record\","
                + "\"name\":\"User\","
                + "\"fields\":["
                + "{\"name\":\"name\", \"type\":\"string\"},"
                + "{\"name\":\"age\", \"type\":\"int\"}"
                + "]"
                + "}";
        Schema avroSchema = new Schema.Parser().parse(schemaStr);

        Path path = new Path(parquetFilePath);
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(avroSchema)
                .build();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(jsonFilePath));

        if (root.isArray()) {
            for (JsonNode node : root) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("name", node.get("name").asText());
                record.put("age", node.get("age").asInt());
                writer.write(record);
            }
        }
        writer.close();
    }
}
