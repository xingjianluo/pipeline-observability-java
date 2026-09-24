package com.github.xingjianluo.pipelineobservabilityjava.model;

/** 管道运行生命周期状态。SUCCESS / FAILED / CANCELLED 为终结态，不可回退。 */
public enum RunStatus {
    PENDING,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == CANCELLED;
    }
}
