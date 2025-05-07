package com.example;




public class App {
    public static void main(String[] args) {
        try (DynamoDBService service = new DynamoDBService()) {
            
            // 添加学生
            //service.addStudent("001", "wanghaoyan", "utd");
            //service.addStudent("002", "kaylee", "washu");
            
            // 查询学生并打印JSON结果
            
            service.getStudent("001");
            service.getStudent("002");
            service.getStudent("003"); // 不存在的学生
        } catch (Exception e) {
            System.err.println("程序异常: " + e.getMessage());
        }
    }
}
