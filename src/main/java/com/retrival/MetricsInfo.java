package com.retrival;

import com.beans.AthenaMetricBean;
import com.beans.QueryStageBean;
import com.beans.QueryStagePlanNodeBean;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class MetricsInfo {
    private static final String QUERY_EXECUTION_ID = "query-execution-id";
    private static final String AWS_ACCESS_KEY = "access-key";
    private static final String AWS_SECRET_KEY = "secret-key";
    private static final Region REGION = Region.AP_SOUTH_1;

    private static AthenaClient createAthenaClient(AwsBasicCredentials awsBasicCredentials) {
        return AthenaClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                .region(REGION)
                .build();
    }

    private static Map<String, String> retrieveDataFromYaml() throws IOException {
        try (InputStream inputStream = MetricsInfo.class.getClassLoader().getResourceAsStream("input_data.yaml")) {
            if (inputStream == null) {
                throw new IOException("YAML file not found in resources");
            }

            Yaml yaml = new Yaml();
            return yaml.load(inputStream);
        }
    }


    private static AthenaMetricBean retrieveAthenaMetrics(AthenaClient athenaClient, String queryExecutionId) {
        GetQueryExecutionResponse queryExecutionResponse = athenaClient.getQueryExecution(
                GetQueryExecutionRequest.builder().queryExecutionId(queryExecutionId).build()
        );

        QueryExecution queryExecution = queryExecutionResponse.queryExecution();
        QueryExecutionStatistics queryExecutionStatistics = queryExecution.statistics();

        GetQueryRuntimeStatisticsResponse queryRuntimeStatisticsResponse = athenaClient.getQueryRuntimeStatistics(
                GetQueryRuntimeStatisticsRequest.builder().queryExecutionId(queryExecutionId).build()
        );

        QueryRuntimeStatistics runtimeStatistics = queryRuntimeStatisticsResponse.queryRuntimeStatistics();
        Instant startTime = queryExecution.status().submissionDateTime();
        Instant endTime = queryExecution.status().completionDateTime();
        long elapsedTimeMillis = Duration.between(startTime, endTime).toMillis();

        QueryStageBean outputStage = mapQueryStage(runtimeStatistics.outputStage());

        return AthenaMetricBean.builder()
                .status(queryExecution.status().stateAsString())
                .dataScannedInBytes(queryExecutionStatistics.dataScannedInBytes())
                .startTime(startTime)
                .endTime(endTime)
                .elapsedTime(elapsedTimeMillis)
                .queuedTime(queryExecutionStatistics.queryQueueTimeInMillis())
                .executionTime(queryExecutionStatistics.engineExecutionTimeInMillis())
                .planningTime(queryExecutionStatistics.queryPlanningTimeInMillis())
                .analysisTime(
                        queryExecutionStatistics.totalExecutionTimeInMillis()
                                - queryExecutionStatistics.engineExecutionTimeInMillis()
                                - queryExecutionStatistics.queryPlanningTimeInMillis())
                .inputRows(runtimeStatistics.rows().inputRows())
                .inputBytes(runtimeStatistics.rows().inputBytes())
                .outputRows(runtimeStatistics.rows().outputRows())
                .outputBytes(runtimeStatistics.rows().outputBytes())
                .queryStageBean(outputStage)
                .build();
    }

    private static QueryStageBean mapQueryStage(QueryStage queryStage) {
        QueryStagePlanNodeBean planNode = mapQueryStagePlanNode(queryStage.queryStagePlan());

        List<QueryStageBean> subStageBeans = new ArrayList<>();
        if (!queryStage.subStages().isEmpty()) {
            for (QueryStage subStage : queryStage.subStages()) {
                subStageBeans.add(mapQueryStage(subStage));
            }
        }

        return QueryStageBean.builder()
                .stageId(queryStage.stageId())
                .state(queryStage.state())
                .inputRows(queryStage.inputRows())
                .inputBytes(queryStage.inputBytes())
                .outputRows(queryStage.outputRows())
                .outputBytes(queryStage.outputBytes())
                .executionTime(queryStage.executionTime())
                .queryStagePlanNodeBean(planNode)
                .subStage(subStageBeans)
                .build();
    }

    private static QueryStagePlanNodeBean mapQueryStagePlanNode(QueryStagePlanNode planNode) {
        List<QueryStagePlanNodeBean> childNodes = new ArrayList<>();
        if (planNode.hasChildren()) {
            for (QueryStagePlanNode child : planNode.children()) {
                childNodes.add(mapQueryStagePlanNode(child));
            }
        }

        return QueryStagePlanNodeBean.builder()
                .name(planNode.name())
                .identifier(planNode.identifier())
                .queryStagePlanNodeBeans(childNodes)
                .build();
    }


    public static void main(String[] args) throws IOException {
        Map<String, String> yamlData = retrieveDataFromYaml();
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(
                yamlData.get(AWS_ACCESS_KEY),
                yamlData.get(AWS_SECRET_KEY)
        );

        try (AthenaClient athenaClient = createAthenaClient(awsBasicCredentials)) {
            AthenaMetricBean metrics = retrieveAthenaMetrics(athenaClient, yamlData.get(QUERY_EXECUTION_ID));
            log.info("Athena Metrics: {}", metrics);
//            System.out.println("metrics : " + metrics.toString());
        }
    }
}
