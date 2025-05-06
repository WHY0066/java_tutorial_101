package com.example;

public class App {
    public static void main(String[] args) {
        DynamoDBService service = new DynamoDBService();

        // 示例：插入一个学生
        service.addStudent("001", "wanghaoyan", "utd");
        service.addStudent("002", "kaylee", "washu");

        // 示例：读取学生
        service.getStudent("001");
        service.getStudent("001");
    }
}
