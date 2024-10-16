import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ScanArtClassQuestions {

    public static void main(String[] args) {
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(region)
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        scanArtClassQuestions(enhancedClient);

        ddb.close();
    }

    private static void scanArtClassQuestions(DynamoDbEnhancedClient enhancedClient) {
        DynamoDbTable<ClassQuestion> table = enhancedClient.table("ClassQuestions", TableSchema.fromBean(ClassQuestion.class));

        Expression filterExpression = Expression.builder()
                .expression("className = :className")
                .putExpressionValue(":className", AttributeValue.builder().s("Art").build())
                .build();

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder()
                .filterExpression(filterExpression)
                .build();

        List<ClassQuestion> artQuestions = new ArrayList<>();

        table.scan(scanRequest).items().forEach(artQuestions::add);

        // Sort the results by student
        artQuestions.sort(Comparator.comparing(ClassQuestion::getStudent));

        // Limit to 10 items
        int limit = Math.min(artQuestions.size(), 10);
        for (int i = 0; i < limit; i++) {
            ClassQuestion item = artQuestions.get(i);
            System.out.println("Question ID: " + item.getQuestionId());
            System.out.println("Timestamp: " + item.getTimestamp());
            System.out.println("Student: " + item.getStudent());
            System.out.println("Teacher: " + item.getTeacher());
            System.out.println("Class: " + item.getClassName());
            System.out.println("Question: " + item.getQuestionContent());
            System.out.println("Remark: " + item.getRemark());
            System.out.println("--------------------");
        }
    }

    @DynamoDbBean
    public static class ClassQuestion {
        private String questionId;
        private Long timestamp;
        private String questionContent;
        private String student;
        private String teacher;
        private String className;
        private String remark;

        @DynamoDbPartitionKey
        public String getQuestionId() {
            return questionId;
        }

        public void setQuestionId(String questionId) {
            this.questionId = questionId;
        }

        @DynamoDbSortKey
        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public String getQuestionContent() {
            return questionContent;
        }

        public void setQuestionContent(String questionContent) {
            this.questionContent = questionContent;
        }

        public String getStudent() {
            return student;
        }

        public void setStudent(String student) {
            this.student = student;
        }

        public String getTeacher() {
            return teacher;
        }

        public void setTeacher(String teacher) {
            this.teacher = teacher;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getRemark() {
            return remark;
        }

        public void setRemark(String remark) {
            this.remark = remark;
        }
    }
}
