package com.github.xingjianluo.pipelineobservabilityjava.observability.model;

/**
 * 单次管道运行（run）经状态收敛后的终态/当前态。
 */
public enum RunStatus {
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    public boolean terminal() {
        return this != RUNNING;
    }
}
