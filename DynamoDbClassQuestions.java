import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;

import java.time.Instant;
import java.util.UUID;

public class DynamoDbClassQuestions {

    public static void main(String[] args) {
        // Create a DynamoDB client
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(region)
                .build();

        // Create a DynamoDB Enhanced Client
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        // Create the table
        createTable(enhancedClient);

        // Insert dummy data
        insertDummyData(enhancedClient);

        ddb.close();
    }

    private static void createTable(DynamoDbEnhancedClient enhancedClient) {
        DynamoDbTable<ClassQuestion> table = enhancedClient.table("ClassQuestions", TableSchema.fromBean(ClassQuestion.class));
        try {
            table.createTable(builder -> builder
                    .provisionedThroughput(b -> b
                            .readCapacityUnits(5L)
                            .writeCapacityUnits(5L)
                            .build())
            );
            System.out.println("Table created successfully");
        } catch (ResourceInUseException e) {
            System.out.println("Table already exists");
        }
    }

    private static void insertDummyData(DynamoDbEnhancedClient enhancedClient) {
        DynamoDbTable<ClassQuestion> table = enhancedClient.table("ClassQuestions", TableSchema.fromBean(ClassQuestion.class));

        String[] classes = {"Math", "Science", "History", "English", "Art"};
        String[] teachers = {"Mr. Smith", "Ms. Johnson", "Mrs. Williams", "Mr. Brown", "Ms. Davis"};

        for (int i = 0; i < 10000; i++) {
            ClassQuestion question = new ClassQuestion();
            question.setQuestionId(UUID.randomUUID().toString());
            question.setTimestamp(Instant.now().toEpochMilli());
            question.setQuestionContent("Sample question content " + i);
            question.setStudent("Student " + (i % 100));
            question.setTeacher(teachers[i % teachers.length]);
            question.setClassName(classes[i % classes.length]);
            question.setRemark("Sample remark for question " + i);

            table.putItem(question);

            if (i % 100 == 0) {
                System.out.println("Inserted " + (i + 1) + " items");
            }
        }
        System.out.println("Finished inserting 10000 items");
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
