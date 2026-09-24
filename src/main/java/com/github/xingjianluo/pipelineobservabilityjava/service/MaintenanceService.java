package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.List;

import com.github.xingjianluo.pipelineobservabilityjava.model.MaintenanceWindow;

/** 维护窗口管理与命中判定。 */
public interface MaintenanceService {

    MaintenanceWindow add(String pipelineName, long startAt, long endAt, String note);

    boolean isSuppressed(String pipelineName, long atMillis);

    List<MaintenanceWindow> list();
}
