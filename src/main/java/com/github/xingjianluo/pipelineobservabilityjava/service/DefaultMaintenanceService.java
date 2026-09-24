package com.github.xingjianluo.pipelineobservabilityjava.service;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Service;

import com.github.xingjianluo.pipelineobservabilityjava.model.MaintenanceWindow;

@Service
public class DefaultMaintenanceService implements MaintenanceService {

    private final ConcurrentMap<String, MaintenanceWindow> windows = new ConcurrentHashMap<>();

    @Override
    public MaintenanceWindow add(String pipelineName, long startAt, long endAt, String note) {
        if (endAt < startAt) {
            throw new IllegalArgumentException("maintenance endAt must be >= startAt");
        }
        MaintenanceWindow w = new MaintenanceWindow();
        w.setId(UUID.randomUUID().toString());
        w.setPipelineName(pipelineName == null || pipelineName.isBlank() ? null : pipelineName);
        w.setStartAt(startAt);
        w.setEndAt(endAt);
        w.setNote(note);
        windows.put(w.getId(), w);
        return w;
    }

    @Override
    public boolean isSuppressed(String pipelineName, long atMillis) {
        // 管道专属窗口或全局窗口（pipelineName 为空）命中即抑制。
        return windows.values().stream()
                .filter(w -> w.getPipelineName() == null || w.getPipelineName().equals(pipelineName))
                .anyMatch(w -> w.contains(atMillis));
    }

    @Override
    public List<MaintenanceWindow> list() {
        return windows.values().stream()
                .sorted(Comparator.comparingLong(MaintenanceWindow::getStartAt))
                .toList();
    }
}
