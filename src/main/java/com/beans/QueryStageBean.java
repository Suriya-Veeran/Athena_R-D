package com.beans;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@Builder
@ToString
public class QueryStageBean {

    private Long stageId;
    private String state;
    private Long inputRows;
    private Long inputBytes;
    private Long outputRows;
    private Long outputBytes;
    private Long executionTime;

    private QueryStagePlanNodeBean queryStagePlanNodeBean;
    private List<QueryStageBean> subStage;




}
