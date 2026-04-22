package ru.sfera.platform.tasks.processor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.util.Strings;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import ru.sfera.platform.config.properties.EntityProperties;
import ru.sfera.platform.core.api.common.exception.NoSuchEntityException;
import ru.sfera.platform.core.api.domains.DomainContextHolder;
import ru.sfera.platform.core.api.entity.model.CopyEntityPayload;
import ru.sfera.platform.core.api.entity.processors.EntityCloneLifecycleProcessor;
import ru.sfera.platform.core.rest.api.v1.attributes.entity.ParentAPIAttribute;
import ru.sfera.platform.core.rest.api.v1.attributes.entity.RelationsAPIAttribute;
import ru.sfera.platform.db.models.entity.JpaRelation;
import ru.sfera.platform.domain.entity.Entity;
import ru.sfera.platform.domain.entity.Relation;
import ru.sfera.platform.history.registrars.RelationsModificationHandler;
import ru.sfera.platform.model.entity.RelationsDomainAttribute;
import ru.sfera.platform.repositories.entity.EntityRelationTypeRepository;
import ru.sfera.platform.service.EntityHistoryService;
import ru.sfera.platform.service.EntityRelationsService;
import ru.sfera.platform.service.domains.DomainService;


@Order
@Component
public class CopyProcessor implements EntityCloneLifecycleProcessor {

    private final EntityProperties entityProperties;
    private final EntityRelationsService relationsService;
    private final EntityRelationTypeRepository entityRelationTypeRepository;
    private final EntityHistoryService entityHistoryService;
    private final RelationsDomainAttribute relationsDomainAttribute;
    private final RelationsAPIAttribute relationsAPIAttribute;
    private final ParentAPIAttribute parentAPIAttribute;
    private final RelationsModificationHandler relationsModificationHandler;
    private final ThreadLocal<Entity> originalEntity = new ThreadLocal<>();
    private final ThreadLocal<Map<String, Object>> fieldsToOverride = new ThreadLocal<>();

    public CopyProcessor(
        EntityProperties entityProperties,
        EntityRelationsService relationsService,
        EntityRelationTypeRepository entityRelationTypeRepository,
        EntityHistoryService entityHistoryService,
        RelationsDomainAttribute relationsDomainAttribute,
        @Lazy RelationsAPIAttribute relationsAPIAttribute,
        @Lazy ParentAPIAttribute parentAPIAttribute,
        RelationsModificationHandler relationsModificationHandler,
        DomainContextHolder domainContextHolder,
        DomainService domainService
    ) {
        this.entityProperties = entityProperties;
        this.relationsService = relationsService;
        this.entityRelationTypeRepository = entityRelationTypeRepository;
        this.entityHistoryService = entityHistoryService;
        this.relationsDomainAttribute = relationsDomainAttribute;
        this.relationsAPIAttribute = relationsAPIAttribute;
        this.parentAPIAttribute = parentAPIAttribute;
        this.relationsModificationHandler = relationsModificationHandler;
        this.domainContextHolder = domainContextHolder;
        this.domainService = domainService;
    }

    private final DomainContextHolder domainContextHolder;
    private final DomainService domainService;

    @Override
    public boolean acceptableInCurrentDomain() {
        return domainContextHolder.get().domain().equals(domainService.getDefault());
    }

    @Override
    public CopyEntityPayload before(CopyEntityPayload request) {
        var fieldsOverride = request.fieldsToOverride();
        if (fieldsOverride == null) {
            return request;
        }
        originalEntity.set(request.original());
        fieldsToOverride.set(fieldsOverride);
        return new CopyEntityPayload(request.original(), request.fieldsToCopy(), fieldsOverride);
    }

    private void bind(String relationTypeCode, Entity clone, Entity relatedEntity) {
        var relationType = entityRelationTypeRepository.findByCode(relationTypeCode)
            .orElseThrow(() ->
                new NoSuchEntityException(Relation.class, relationTypeCode));
        relationsService.createEntityRelation(clone, relatedEntity, relationType, null);
    }

    @Override
    public Entity after(Entity clone) {
        var fields = fieldsToOverride.get();
        var parentOverride = fields.get(parentAPIAttribute.getCode());
        var hasRelations = fields.get(relationsAPIAttribute.getCode()) != null;
        var relations = extractRelations(fields);
        boolean hasOriginalEntityRelation = relations.stream()
            .anyMatch(rel -> originalEntity.get().getNumber().equals(rel.getRelatedEntity().getNumber()));
        var originalRelations = getCurrentRelations(clone);
        var cloneRelationTypeCode = entityProperties.getCloneRelationTypeCode();

        if (!Strings.isBlank(cloneRelationTypeCode)) {
            if (hasRelations && hasOriginalEntityRelation) {
                originalRelations = originalRelations.stream()
                    .filter(rel -> !originalEntity.get().getNumber().equals(rel.getEntity().getNumber()))
                    .collect(Collectors.toList());
                relationsModificationHandler.skipHistoryCreationFromEntity(originalEntity.get());
            }

            if (parentOverride == null && !hasOriginalEntityRelation) {
                bind(cloneRelationTypeCode, clone, originalEntity.get());
            }
        }
        entityHistoryService.registerEntityUpdate(
            Map.of(
                clone,
                Map.of(relationsDomainAttribute.getCode(), originalRelations)
            ), Map.of()
        );

        return clone;
    }

    private List<JpaRelation> extractRelations(Map<String, Object> fields) {
        var obj = fields.get(relationsAPIAttribute.getCode());
        if (obj instanceof List<?> list) {
            if (list.isEmpty() || list.get(0) instanceof JpaRelation) {
                return (List<JpaRelation>) list;
            }
        }
        return List.of();
    }

    private List<JpaRelation> getCurrentRelations(Entity entity) {
        Page<? extends Relation> relationsPage =
            relationsService.getDirectRelations(entity, null, Pageable.unpaged());
        return relationsPage.getContent().stream()
            .map(relation -> (JpaRelation) relation)
            .collect(Collectors.toList());
    }

}
