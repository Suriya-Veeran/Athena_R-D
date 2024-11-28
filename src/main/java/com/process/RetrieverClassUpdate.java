package com.process;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;

@Slf4j
public class RetrieverClassUpdate {

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

  private static CloudWatchClient createCloudWatchClient(AwsBasicCredentials awsBasicCredentials) {
    return CloudWatchClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
        .region(REGION)
        .build();
  }

  private static Map<String, String> retrieveDataFromYaml() throws IOException {
    try (InputStream inputStream =
        new FileInputStream(
            "/home/p3/IdeaProjects/Test_projects/src/main/resources/input-data.yaml")) {
      Yaml yaml = new Yaml();
      return yaml.load(inputStream);
    }
  }

  public static void main(String[] args) throws IOException {
    Map<String, String> yamlData = retrieveDataFromYaml();
    AwsBasicCredentials awsBasicCredentials =
        AwsBasicCredentials.create(yamlData.get(AWS_ACCESS_KEY), yamlData.get(AWS_SECRET_KEY));

    try (AthenaClient athenaClient = createAthenaClient(awsBasicCredentials);
        CloudWatchClient cloudWatchClient = createCloudWatchClient(awsBasicCredentials)) {

      retrieveAthenaBasicInfo(athenaClient, yamlData.get(QUERY_EXECUTION_ID));
    }
  }

  private static void retrieveAthenaBasicInfo(AthenaClient athenaClient,
                                              String queryExecutionId) {

    GetQueryExecutionResponse queryExecutionResponse =
        athenaClient.getQueryExecution(
            GetQueryExecutionRequest.builder().queryExecutionId(queryExecutionId).build());

    QueryExecution queryExecution = queryExecutionResponse.queryExecution();
    QueryExecutionStatistics queryExecutionStatistics = queryExecution.statistics();

    GetQueryRuntimeStatisticsResponse queryRuntimeStatistics =
        athenaClient.getQueryRuntimeStatistics(
            GetQueryRuntimeStatisticsRequest.builder().queryExecutionId(queryExecutionId).build());

    System.out.println("query status: " + queryExecution.status().state());
    System.out.println("query bytes scanned : " + queryExecutionStatistics.dataScannedInBytes());
    /// /////////////////////////////////////////////////////////
    Instant startTime = queryExecution.status().submissionDateTime();
    Instant endTime = queryExecution.status().completionDateTime();
    long elapsedTimeInMillis = Duration.between(startTime, endTime).toMillis();
    long analysisTimeInMillis =
        queryExecutionStatistics.totalExecutionTimeInMillis()
            - queryExecutionStatistics.engineExecutionTimeInMillis()
            - queryExecutionStatistics.queryPlanningTimeInMillis();

    System.out.println("query submission date: " + startTime);
    System.out.println("query completion date: " + endTime);
    System.out.println("Elapsed time : " + elapsedTimeInMillis);
    System.out.println("Query Que time : " + queryExecutionStatistics.queryQueueTimeInMillis());
    System.out.println("Query Analysis time : " + analysisTimeInMillis);
    System.out.println("Planning Time : " + queryExecutionStatistics.queryPlanningTimeInMillis());
    System.out.println(
        "execution time : " + queryExecutionStatistics.engineExecutionTimeInMillis());

    System.out.println(
        "Query Run time statistics " + queryRuntimeStatistics.queryRuntimeStatistics());
    System.out.println(
        "Query Input rows : " + queryRuntimeStatistics.queryRuntimeStatistics().rows().inputRows());
    System.out.println(
        "Query Input bytes : "
            + queryRuntimeStatistics.queryRuntimeStatistics().rows().inputBytes());
    System.out.println(
        "Query output rows : "
            + queryRuntimeStatistics.queryRuntimeStatistics().rows().outputRows());
    System.out.println(
        "Query output bytes : "
            + queryRuntimeStatistics.queryRuntimeStatistics().rows().outputBytes());

    System.out.println(
        " *********************************Stages ***************************************");
    System.out.println(
        "Query Output Stage id : "
            + queryRuntimeStatistics.queryRuntimeStatistics().outputStage().stageId());
    System.out.println(
        "Query Output Stage State : "
            + queryRuntimeStatistics.queryRuntimeStatistics().outputStage().state());
    System.out.println(
        "Query Output Stage Output rows : "
            + queryRuntimeStatistics.queryRuntimeStatistics().outputStage().outputRows());
    System.out.println(
        "Query Output Stage Output bytes : "
            + queryRuntimeStatistics.queryRuntimeStatistics().outputStage().outputBytes());
    System.out.println(
        "Query Output Stage input rows : "
            + queryRuntimeStatistics.queryRuntimeStatistics().outputStage().inputRows());
    System.out.println(
        "Query Output Stage input bytes : "
            + queryRuntimeStatistics.queryRuntimeStatistics().outputStage().inputBytes());
    System.out.println(
        "Query Output Stage execution time : "
            + queryRuntimeStatistics.queryRuntimeStatistics().outputStage().executionTime());

    System.out.println("******Operators********************");
    QueryStagePlanNode queryStagePlanNode =
        queryRuntimeStatistics.queryRuntimeStatistics().outputStage().queryStagePlan();
    System.out.println("query output stage operator name : " + queryStagePlanNode.name());
    System.out.println("query output stage identifier : " + queryStagePlanNode.identifier());
    getQueryStagePlan(queryStagePlanNode);
    List<QueryStage> queryStages =
        queryRuntimeStatistics.queryRuntimeStatistics().outputStage().subStages();
    if (!queryStages.isEmpty()) {
      retrieveQueryStages(queryStages);
    }
  }

  private static void retrieveQueryStages(List<QueryStage> queryStages) {
    for (QueryStage queryStage : queryStages) {
      System.out.println("Stage Id : " + "Stage -> " + queryStage.stageId());
      System.out.println("Sub stage State : " + queryStage.state());
      System.out.println("Sub Stage Output rows " + queryStage.outputRows());
      System.out.println("Sub Stage Output bytes " + queryStage.outputBytes());
      System.out.println("Sub Stage input rows " + queryStage.inputRows());
      System.out.println("Sub Stage input bytes " + queryStage.inputBytes());
      System.out.println("Sub Stage execution time " + queryStage.executionTime());
      getQueryStagePlan(queryStage.queryStagePlan());
    }
  }

  private static void getQueryStagePlan(QueryStagePlanNode queryStagePlanNode) {

    if (queryStagePlanNode.hasChildren()) {
      for (QueryStagePlanNode child : queryStagePlanNode.children()) {
        System.out.println("query output stage children : " + child.name());
        System.out.println("query output stage identifier : " + child.identifier());
        if (child.hasChildren()) {
          getQueryStagePlan(child);
        }
      }
    }
  }
}
