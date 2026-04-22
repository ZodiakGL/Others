package ru.sfera.platform.goal.counter.service.impl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.sfera.platform.goal.counter.db.repositories.projections.OverdueGoalCounterProjection;
import ru.sfera.platform.goal.counter.dto.CounterCalcRecord;
import ru.sfera.platform.goal.counter.dto.TaskToGoalsCountersRecord;
import ru.sfera.platform.goal.counter.service.CounterCalculateService;
import ru.sfera.platform.goal.counter.service.GoalCounterService;
import ru.sfera.platform.goal.counter.service.OverdueGoalCountersService;
import ru.sfera.platform.goal.counter.service.ScheduledTaskService;
import ru.sfera.platform.timer.template.db.models.entity.Calendar;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScheduledTaskServiceImpl implements ScheduledTaskService {

    @Value("${sfera.tasks.sla.counter.scheduler.asyncThreadsCount:4}")
    private int threadCount;
    private ExecutorService executorService;

    private static final int PAGE_SIZE = 1000;
    private final GoalCounterService goalCounterService;
    private final CounterCalculateService counterCalculateService;
    private final OverdueGoalCountersService overdueGoalCountersService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        executorService = Executors.newFixedThreadPool(threadCount);
    }

    @Override
    public void executeTask() {
        OverdueGoalCounterProjection lastStoredRecord = null;

        while (true) {
            var createdDate = lastStoredRecord == null ? null : lastStoredRecord.getCreatedDate();
            List<OverdueGoalCounterProjection> notExpiredGoalCounters = goalCounterService
                    .findNotExpiredGoalCounters(createdDate, PAGE_SIZE);
            if (notExpiredGoalCounters.isEmpty()) {
                break;
            }
            
            List<OverdueGoalCounterProjection> expiredGoalCounters = notExpiredGoalCounters.stream()
                    .filter(this::isGoalCounterAlreadyExpired)
                    .toList();
            
            if (!expiredGoalCounters.isEmpty()) {
                processTasksUpdate(extractTaskToGoalsCountersRecords(expiredGoalCounters));
            }
            
            OverdueGoalCounterProjection lastRecord = notExpiredGoalCounters.get(notExpiredGoalCounters.size() - 1);

            if (lastStoredRecord != null && lastRecord.getId().equals(lastStoredRecord.getId())) {
                break;
            }
            lastStoredRecord = lastRecord;
        }
    }

    private List<TaskToGoalsCountersRecord> extractTaskToGoalsCountersRecords(List<OverdueGoalCounterProjection> goalCounterProjections) {
        return goalCounterProjections.stream()
                .collect(Collectors.groupingBy(OverdueGoalCounterProjection::getTaskId))
                .entrySet().stream()
                .map(entry -> new TaskToGoalsCountersRecord(entry.getKey(), entry.getValue()))
                .toList();
    }

    private void processTasksUpdate(List<TaskToGoalsCountersRecord> taskToGoalsCountersRecords) {
        List<CompletableFuture<Void>> completableFutures = taskToGoalsCountersRecords.stream()
                .map(projection -> CompletableFuture.runAsync(() -> {
                    try {
                        expireGoalCountersAndUpdateTasks(projection);
                    } catch (Exception e) {
                        log.error("Ошибка при обновлении задачи!", e);
                    }
                }, executorService)).toList();
        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).join();
    }

    private boolean isGoalCounterAlreadyExpired(OverdueGoalCounterProjection overdueGoalCounterProjection) {
        Long elapsedTime = overdueGoalCounterProjection.getElapsedTime();
        List<Calendar> calendars;
        try {
            calendars = objectMapper.readValue(overdueGoalCounterProjection.getCalendars(), new TypeReference<>(){});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        CounterCalcRecord counterCalcRecord = new CounterCalcRecord(null,
                overdueGoalCounterProjection.getStartWorkDay(),
                overdueGoalCounterProjection.getEndWorkDay(),
                overdueGoalCounterProjection.getStartDate(),
                null,
                overdueGoalCounterProjection.getDurationTime(),
                elapsedTime != null ? elapsedTime : 0,
                overdueGoalCounterProjection.getId(),
                Objects.isNull(calendars) ? null : calendars.stream().collect(Collectors.toMap(Calendar::getYear, Calendar::getCode)));
        return LocalDateTime.now().isAfter(counterCalculateService.calculateTaskEnd(counterCalcRecord));
    }

    private void expireGoalCountersAndUpdateTasks(TaskToGoalsCountersRecord taskToGoalsCountersRecord) {
        List<OverdueGoalCounterProjection> goalCounterProjections = taskToGoalsCountersRecord.goalCounterProjections();
        overdueGoalCountersService.overdueGoalCounters(goalCounterProjections);
        log.debug("Обновляем задачи, у которых просрочен таймер.");
        goalCounterProjections.forEach(overdueGoalsProjection -> {
            try {
                overdueGoalCountersService.processOverdueGoal(overdueGoalsProjection);
            } catch (Exception e) {
                log.error("Ошибка обновления задачи.", e);
            }
        });
    }

    @PreDestroy
    public void destroy() {
        executorService.shutdown();
    }
}
