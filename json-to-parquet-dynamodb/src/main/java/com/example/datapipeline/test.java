package com.example.datapipeline;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class test {
    private static final Logger logger = LogManager.getLogger(test.class);

    // 这里写死了路径，也可以改成传参
    private static final String INPUT_JSON = "data.json";
    private static final String OUTPUT_PARQUET = "data.parquet";

    // 你的 S3 信息，执行时可以替换成实际值，或者改成命令行参数
    private static final String BUCKET_NAME = "s3exercisebucketwhy";
    private static final String S3_KEY1 = "data/data.parquet";
    private static final String S3_KEY2 = "data/data.json";
    private static final String REGION = "EU_NORTH_1";
    private static final String DYNAMODB_TABLE = "tests3todynamodb";
    public static void main(String[] args) {
        try {
            

            //JsonToParquetConverter converter = new JsonToParquetConverter();
            
            //JsonToParquetConverter.main(new String[]{});

            
            S3Uploader uploader = new S3Uploader();
           // uploader.uploadFile(BUCKET_NAME, S3_KEY1, OUTPUT_PARQUET);
            //uploader.uploadFile(BUCKET_NAME, S3_KEY2, INPUT_JSON);

            //logger.info("4️⃣ Parquet 上传到 S3 成功");

            S3ToDynamoDB.run(REGION, BUCKET_NAME, S3_KEY2, DYNAMODB_TABLE);
            logger.info("JSON 文件已写入 DynamoDB");
            uploader.close();

        } catch (Exception e) {
            logger.error("fail", e);
        }
    }
}
