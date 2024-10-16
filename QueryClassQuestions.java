import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

import java.util.List;
import java.util.stream.Collectors;

public class QueryClassQuestions {

    public static void main(String[] args) {
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(region)
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        queryArtClassQuestions(enhancedClient);

        ddb.close();
    }

    private static void queryArtClassQuestions(DynamoDbEnhancedClient enhancedClient) {
        DynamoDbTable<ClassQuestion> table = enhancedClient.table("ClassQuestions", TableSchema.fromBean(ClassQuestion.class));

        QueryConditional queryConditional = QueryConditional
                .keyEqualTo(Key.builder().partitionValue("Art").build());

        // Use the GSI
        DynamoDbIndex<ClassQuestion> index = table.index("className-timestamp-index");

        // Perform the query
        SdkIterable<Page<ClassQuestion>> results = index.query(r -> r
                .queryConditional(queryConditional)
                .limit(10));

        // Process and print results
        List<ClassQuestion> questions = results.stream()
                .flatMap(page -> page.items().stream())
                .sorted((q1, q2) -> q1.getStudent().compareTo(q2.getStudent()))
                .limit(10)
                .collect(Collectors.toList());

        System.out.println("Art class questions (sorted by student):");
        for (ClassQuestion question : questions) {
            System.out.printf("Student: %s, Question: %s, Teacher: %s%n",
                    question.getStudent(), question.getQuestionContent(), question.getTeacher());
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
        public String getQuestionId() { return questionId; }
        public void setQuestionId(String questionId) { this.questionId = questionId; }

        @DynamoDbSortKey
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

        @DynamoDbSecondaryPartitionKey(indexNames = "className-timestamp-index")
        public String getClassName() { return className; }
        public void setClassName(String className) { this.className = className; }

        @DynamoDbSecondarySortKey(indexNames = "className-timestamp-index")
        public Long getIndexTimestamp() { return timestamp; }
        public void setIndexTimestamp(Long timestamp) { this.timestamp = timestamp; }

        public String getQuestionContent() { return questionContent; }
        public void setQuestionContent(String questionContent) { this.questionContent = questionContent; }

        public String getStudent() { return student; }
        public void setStudent(String student) { this.student = student; }

        public String getTeacher() { return teacher; }
        public void setTeacher(String teacher) { this.teacher = teacher; }

        public String getRemark() { return remark; }
        public void setRemark(String remark) { this.remark = remark; }
    }
}
