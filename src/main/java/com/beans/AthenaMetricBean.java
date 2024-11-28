package com.beans;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@Builder
@ToString
public class AthenaMetricBean {

    private String status;
    private Long dataScannedInBytes;
    private Instant startTime;
    private Instant endTime;
    private Long elapsedTime;
    private Long queuedTime;
    private Long executionTime;
    private Long planningTime;
    private Long analysisTime;

    private String query;
    private String workGroup;

    private Long inputRows;
    private Long inputBytes;
    private Long outputRows;
    private Long outputBytes;

    private QueryStageBean queryStageBean;


}
