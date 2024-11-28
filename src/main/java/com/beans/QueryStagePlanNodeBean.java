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
public class QueryStagePlanNodeBean {
    private String name;
    private String identifier;
    private List<QueryStagePlanNodeBean> queryStagePlanNodeBeans;
}
