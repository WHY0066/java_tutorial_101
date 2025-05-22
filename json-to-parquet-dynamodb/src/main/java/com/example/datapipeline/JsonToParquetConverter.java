package com.example.datapipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Iterator;

public class JsonToParquetConverter {

    private static final Logger logger = LogManager.getLogger(JsonToParquetConverter.class);

    private static final String INPUT_JSON = "data.json";
    private static final String OUTPUT_PARQUET = "data.parquet";

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();

        try {
           // logger.info("🔍 读取 JSON 文件: {}", INPUT_JSON);
            JsonNode root = mapper.readTree(new File(INPUT_JSON));

            if (!root.isArray()) {
                //logger.error("JSON 根节点不是数组格式！");
                return;
            }

            JsonNode sampleNode = root.get(0);
            Schema avroSchema = buildSchemaFromJson(sampleNode);
           

            Path outputPath = new Path(OUTPUT_PARQUET);
            ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(outputPath)
                    .withSchema(avroSchema)
                    .build();

            for (JsonNode node : root) {
                GenericRecord record = new GenericData.Record(avroSchema);
                Iterator<String> fieldNames = node.fieldNames();
                while (fieldNames.hasNext()) {
                    String field = fieldNames.next();
                    record.put(field, node.get(field).asText());
                }
                writer.write(record);
            }

            writer.close();
            

        } catch (Exception e) {
            logger.error("error in JsonToParquet", e.getMessage(), e);
        }
    }

    private static Schema buildSchemaFromJson(JsonNode node) {
        StringBuilder schemaStr = new StringBuilder();
        schemaStr.append("{\n")
                .append("  \"type\": \"record\",\n")
                .append("  \"name\": \"DataRecord\",\n")
                .append("  \"fields\": [\n");

        Iterator<String> fields = node.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            schemaStr.append("    { \"name\": \"").append(field).append("\", \"type\": \"string\" }");
            if (fields.hasNext()) {
                schemaStr.append(",");
            }
            schemaStr.append("\n");
        }

        schemaStr.append("  ]\n}");
        return new Schema.Parser().parse(schemaStr.toString());
    }
}
