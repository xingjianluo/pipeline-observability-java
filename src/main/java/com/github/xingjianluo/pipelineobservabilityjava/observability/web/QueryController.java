package com.github.xingjianluo.pipelineobservabilityjava.observability.web;

import com.github.xingjianluo.pipelineobservabilityjava.observability.model.PipelineRuntime;
import com.github.xingjianluo.pipelineobservabilityjava.observability.model.RunState;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.PipelineRegistry;
import com.github.xingjianluo.pipelineobservabilityjava.observability.store.RunStateStore;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 管道与运行状态查询接口。 */
@RestController
@RequestMapping("/api")
public class QueryController {

    private final PipelineRegistry registry;
    private final RunStateStore runStateStore;

    public QueryController(PipelineRegistry registry, RunStateStore runStateStore) {
        this.registry = registry;
        this.runStateStore = runStateStore;
    }

    @GetMapping("/pipelines")
    public List<PipelineRuntime> pipelines() {
        return registry.all();
    }

    @GetMapping("/pipelines/{pipelineId}")
    public PipelineRuntime pipeline(@PathVariable String pipelineId) {
        return registry.get(pipelineId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "pipeline not found: " + pipelineId));
    }

    @GetMapping("/pipelines/{pipelineId}/runs")
    public List<RunState> runs(@PathVariable String pipelineId) {
        return runStateStore.findByPipeline(pipelineId);
    }

    @GetMapping("/runs/{runId}")
    public RunState run(@PathVariable String runId) {
        return runStateStore.get(runId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "run not found: " + runId));
    }
}
