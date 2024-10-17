package com.linagora.tmail.event;

import java.util.Collection;

import jakarta.inject.Inject;

import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BucketName;
import org.apache.james.events.Event;
import org.apache.james.events.EventSerializer;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linagora.tmail.blob.secondaryblobstore.FailedBlobEvents;
import com.linagora.tmail.blob.secondaryblobstore.ObjectStorageIdentity;

public class TmailEventSerializer implements EventSerializer {

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = BlobAdditionDTO.class),
        @JsonSubTypes.Type(value = BlobsDeletionDTO.class),
        @JsonSubTypes.Type(value = BucketDeletionDTO.class)
    })
    interface EventDTO {
    }

    record BlobAdditionDTO(String eventId, String username, String bucketName, String blobId, String failedObjectStorage) implements EventDTO {
    }

    record BlobsDeletionDTO(String eventId, String username, String bucketName, Collection<String> blobIds, String failedObjectStorage) implements EventDTO {
    }

    record BucketDeletionDTO(String eventId, String username, String bucketName, String failedObjectStorage) implements EventDTO {
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobId.Factory blobIdFactory;

    @Inject
    public TmailEventSerializer(BlobId.Factory blobIdFactory) {
        this.blobIdFactory = blobIdFactory;
    }

    @Override
    public String toJson(Event event) {
        try {
            EventDTO eventDTO = toDTO(event);
            return objectMapper.writeValueAsString(eventDTO);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Event asEvent(String serialized) {
        try {
            EventDTO eventDTO = objectMapper.readValue(serialized, EventDTO.class);
            return fromDTO(eventDTO);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private EventDTO toDTO(Event event) {
        return switch (event) {
            case FailedBlobEvents.BlobAddition e -> new BlobAdditionDTO(e.getEventId().getId().toString(), FailedBlobEvents.BlobEvent.USERNAME.asString(),
                e.bucketName().asString(), e.blobId().asString(), e.getFailedObjectStorage().name());
            case FailedBlobEvents.BlobsDeletion e -> new BlobsDeletionDTO(e.getEventId().getId().toString(), FailedBlobEvents.BlobEvent.USERNAME.asString(),
                e.bucketName().asString(), e.blobIds().stream().map(BlobId::asString).toList(),
                e.getFailedObjectStorage().name());
            case FailedBlobEvents.BucketDeletion e -> new BucketDeletionDTO(e.getEventId().getId().toString(), FailedBlobEvents.BlobEvent.USERNAME.asString(),
                e.bucketName().asString(), e.getFailedObjectStorage().name());
            default -> throw new IllegalStateException("Unexpected value: " + event);
        };
    }

    private Event fromDTO(EventDTO eventDTO) {
        return switch (eventDTO) {
            case BlobAdditionDTO dto -> new FailedBlobEvents.BlobAddition(Event.EventId.of(dto.eventId()),
                BucketName.of(dto.bucketName()), blobIdFactory.parse(dto.blobId()), ObjectStorageIdentity.valueOf(dto.failedObjectStorage()));
            case BlobsDeletionDTO dto -> new FailedBlobEvents.BlobsDeletion(Event.EventId.of(dto.eventId()),
                BucketName.of(dto.bucketName()), dto.blobIds().stream().map(blobId -> blobIdFactory.parse(blobId)).toList(),
                ObjectStorageIdentity.valueOf(dto.failedObjectStorage()));
            case BucketDeletionDTO dto -> new FailedBlobEvents.BucketDeletion(Event.EventId.of(dto.eventId()),
                BucketName.of(dto.bucketName()), ObjectStorageIdentity.valueOf(dto.failedObjectStorage()));
            default -> throw new IllegalStateException("Unexpected value: " + eventDTO);
        };
    }
}
