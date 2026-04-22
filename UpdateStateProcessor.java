package ru.sfera.platform.tasks.processor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import ru.sfera.platform.api.model.properties.SprintDomainAttribute;
import ru.sfera.platform.core.api.domains.DomainContextHolder;
import ru.sfera.platform.core.api.entity.model.EntityAttributes;
import ru.sfera.platform.core.api.entity.processors.EntityUpdateLifecycleProcessor;
import ru.sfera.platform.domain.entity.Entity;
import ru.sfera.platform.domain.enums.EntityState;
import ru.sfera.platform.model.entity.ChildrenDomainAttribute;
import ru.sfera.platform.model.entity.ParentDomainAttribute;
import ru.sfera.platform.model.entity.RelationsDomainAttribute;
import ru.sfera.platform.model.entity.StateDomainAttribute;
import ru.sfera.platform.service.domains.DomainService;
import ru.sfera.platform.service.impl.EntityHistoryServiceImpl;

import static java.lang.Integer.MAX_VALUE;


/**
 * Вызываем в последнюю очередь перед EntityRankUpdateLifecycleProcessor и UpdateHistoryLifecycleProcessor.
 */
@Order(MAX_VALUE - 2)
@Slf4j
@Component
@RequiredArgsConstructor
public class UpdateStateProcessor implements EntityUpdateLifecycleProcessor {

    private final StateDomainAttribute stateDomainAttribute;
    private final SprintDomainAttribute sprintDomainAttribute;
    private final ParentDomainAttribute parentDomainAttribute;
    private final RelationsDomainAttribute relationsDomainAttribute;
    private final ChildrenDomainAttribute childrenDomainAttribute;
    private final EntityHistoryServiceImpl historyService;
    private final ThreadLocal<EntityState> oldStateHolder = new ThreadLocal<>();

    private final DomainContextHolder domainContextHolder;
    private final DomainService domainService;

    @Override
    public boolean acceptableInCurrentDomain() {
        return domainContextHolder.get().domain().equals(domainService.getDefault());
    }

    @Override
    public EntityAttributes before(EntityAttributes entityAttributes) {
        var entity = entityAttributes.entity();
        oldStateHolder.set(entity.getState());
        var newState = (EntityState) entityAttributes.attributes().get(stateDomainAttribute.getCode());
        var attrs = new HashMap<>(entityAttributes.attributes());
        var modifications = new HashSet<>(entityAttributes.modifications());
        if (needClearSprint(entity.getState(), newState)) {
            log.info("Actual sprint will be removed from entity with id '%s'".formatted(entity.getId()));
            attrs.put(sprintDomainAttribute.getCode(), List.of());
            modifications.removeIf(mod -> mod.getCode().equals(sprintDomainAttribute.getCode()));
        }

        if (needClearRelations(entity, newState)) {
            log.info("Actual relations, parent and children will be removed from entity with id '%s'".formatted(entity.getId()));
            var oldRelations = relationsDomainAttribute.get(entity);
            var oldChildren = childrenDomainAttribute.get(entity);
            var oldParent = parentDomainAttribute.get(entity);

            childrenDomainAttribute.clear(entity);
            parentDomainAttribute.clear(entity);
            oldRelations.forEach(relation ->
                relationsDomainAttribute.clear(entity, relation.getId().toString())
            );
            attrs.remove(relationsDomainAttribute.getCode());
            attrs.remove(childrenDomainAttribute.getCode());
            attrs.remove(parentDomainAttribute.getCode());
            modifications.removeIf(mod ->
                mod.getCode().equals(relationsDomainAttribute.getCode())
                || mod.getCode().equals(childrenDomainAttribute.getCode())
                || mod.getCode().equals(parentDomainAttribute.getCode()));

            if (oldChildren != null && !oldChildren.isEmpty()) {
                var entityUpdates = oldChildren.stream()
                    .collect(Collectors.toMap(
                        Entity.class::cast,
                        child -> Map.<String, Object>of(parentDomainAttribute.getCode(), entity)
                    ));
                historyService.registerEntityUpdate(entityUpdates, Map.of());
            }
            historyService.registerEntityUpdate(Map.of(entity, Map.of(relationsDomainAttribute.getCode(), oldRelations)), Map.of());
            historyService.registerEntityUpdate(Map.of(entity, Map.of(childrenDomainAttribute.getCode(), oldChildren)), Map.of());

            if (oldParent != null) {
                historyService.registerEntityUpdate(Map.of(entity, Map.of(parentDomainAttribute.getCode(), oldParent)), Map.of());
            }
        }
        return new EntityAttributes(entity, attrs, modifications);
    }

    /**
     * Удаляем спринт только если state изменился с normal на Archived или Deleted.
     */
    private boolean needClearSprint(EntityState oldState, EntityState newState) {
        return oldState.isNormal() && (newState == EntityState.Archived || newState == EntityState.Deleted);
    }

    /**
     * Удаляем связи только если state изменился на Deleted.
     */
    private boolean needClearRelations(Entity entity, EntityState newState) {
        return (entity.getState() != EntityState.Deleted) && (newState == EntityState.Deleted);
    }

    @Override
    public Entity after(Entity entity) {
        try {
            var newState = entity.getState();
            var oldState = oldStateHolder.get();
            if (needClearSprint(oldState, newState)) {
                log.info("Actual sprint removed from entity with id '%s'".formatted(entity.getId()));
                sprintDomainAttribute.clear(entity);
            }
            return entity;
        } finally {
            oldStateHolder.remove();
        }
    }

}
