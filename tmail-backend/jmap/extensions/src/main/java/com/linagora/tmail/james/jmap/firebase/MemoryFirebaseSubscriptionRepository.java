package com.linagora.tmail.james.jmap.firebase;

import static com.linagora.tmail.james.jmap.firebase.FirebaseSubscriptionHelper.evaluateExpiresTime;
import static com.linagora.tmail.james.jmap.firebase.FirebaseSubscriptionHelper.isInThePast;
import static com.linagora.tmail.james.jmap.firebase.FirebaseSubscriptionHelper.isNotOutdatedSubscription;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import org.apache.james.core.Username;
import org.apache.james.jmap.api.model.TypeName;
import org.apache.james.user.api.DeleteUserDataTaskStep;
import org.reactivestreams.Publisher;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.linagora.tmail.james.jmap.model.DeviceClientIdInvalidException;
import com.linagora.tmail.james.jmap.model.ExpireTimeInvalidException;
import com.linagora.tmail.james.jmap.model.FirebaseSubscription;
import com.linagora.tmail.james.jmap.model.FirebaseSubscriptionCreationRequest;
import com.linagora.tmail.james.jmap.model.FirebaseSubscriptionExpiredTime;
import com.linagora.tmail.james.jmap.model.FirebaseSubscriptionId;
import com.linagora.tmail.james.jmap.model.FirebaseSubscriptionNotFoundException;
import com.linagora.tmail.james.jmap.model.TokenInvalidException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

public class MemoryFirebaseSubscriptionRepository implements FirebaseSubscriptionRepository {

    public static class Module extends AbstractModule {
        @Override
        protected void configure() {
            bind(MemoryFirebaseSubscriptionRepository.class).in(Scopes.SINGLETON);
            bind(FirebaseSubscriptionRepository.class).to(MemoryFirebaseSubscriptionRepository.class);

            Multibinder.newSetBinder(binder(), DeleteUserDataTaskStep.class)
                .addBinding()
                .to(FirebaseSubscriptionUserDeletionTaskStep.class);
        }
    }

    private final Table<Username, FirebaseSubscriptionId, FirebaseSubscription> table;
    private final Clock clock;

    @Inject
    public MemoryFirebaseSubscriptionRepository(Clock clock) {
        this.clock = clock;
        this.table = HashBasedTable.create();
    }

    @Override
    public Publisher<FirebaseSubscription> save(Username username, FirebaseSubscriptionCreationRequest request) {
        return Mono.just(request)
            .handle((req, sink) -> {
                if (isInThePast(req.expires(), clock)) {
                    sink.error(new ExpireTimeInvalidException(req.expires().get().value(), "expires must be greater than now"));
                }
                if (!isUniqueDeviceClientId(username, req.deviceClientId())) {
                    sink.error(new DeviceClientIdInvalidException(req.deviceClientId(), "deviceClientId must be unique"));
                }
                if (!isUniqueDeviceToken(username, req.token())) {
                    sink.error(new TokenInvalidException("deviceToken must be unique"));
                }
            })
            .thenReturn(FirebaseSubscription.from(request,
                evaluateExpiresTime(OptionConverters.toJava(request.expires().map(FirebaseSubscriptionExpiredTime::value)),
                    clock)))
            .doOnNext(firebaseSubscription -> table.put(username, firebaseSubscription.id(), firebaseSubscription));
    }

    @Override
    public Publisher<FirebaseSubscriptionExpiredTime> updateExpireTime(Username username, FirebaseSubscriptionId id, ZonedDateTime newExpire) {
        return Mono.just(newExpire)
            .handle((inputTime, sink) -> {
                if (newExpire.isBefore(ZonedDateTime.now(clock))) {
                    sink.error(new ExpireTimeInvalidException(inputTime, "expires must be greater than now"));
                }
            })
            .then(Mono.justOrEmpty(table.get(username, id))
                .mapNotNull(oldSubscription -> {
                    FirebaseSubscription updatedSubscription = oldSubscription.withExpires(evaluateExpiresTime(Optional.of(newExpire), clock));
                    table.put(username, id, updatedSubscription);
                    return updatedSubscription;
                })
                .map(FirebaseSubscription::expires)
                .switchIfEmpty(Mono.error(() -> new FirebaseSubscriptionNotFoundException(id))));
    }

    @Override
    public Publisher<Void> updateTypes(Username username, FirebaseSubscriptionId id, Set<TypeName> types) {
        return Mono.justOrEmpty(table.get(username, id))
            .doOnNext(oldSubscription -> {
                FirebaseSubscription updatedSubscription = oldSubscription.withTypes(CollectionConverters.asScala(types).toSeq());
                table.put(username, id, updatedSubscription);
            })
            .switchIfEmpty(Mono.error(() -> new FirebaseSubscriptionNotFoundException(id)))
            .then();
    }

    @Override
    public Publisher<Void> revoke(Username username, FirebaseSubscriptionId id) {
        return Mono.fromCallable(() -> table.remove(username, id)).then();
    }

    @Override
    public Publisher<Void> revoke(Username username) {
        return Mono.fromRunnable(() -> table.row(username).clear())
            .then();
    }

    @Override
    public Publisher<FirebaseSubscription> get(Username username, Set<FirebaseSubscriptionId> ids) {
        return Flux.fromIterable(table.row(username).entrySet())
            .filter(entry -> ids.contains(entry.getKey()))
            .map(Map.Entry::getValue)
            .filter(subscription -> isNotOutdatedSubscription(subscription, clock));
    }

    @Override
    public Publisher<FirebaseSubscription> list(Username username) {
        return Flux.fromIterable(table.row(username).entrySet())
            .map(Map.Entry::getValue)
            .filter(subscription -> isNotOutdatedSubscription(subscription, clock));
    }

    private boolean isUniqueDeviceClientId(Username username, String deviceClientId) {
        return table.row(username).values().stream()
            .noneMatch(subscription -> subscription.deviceClientId().equals(deviceClientId));
    }

    private boolean isUniqueDeviceToken(Username username, String deviceToken) {
        return table.row(username).values().stream()
            .noneMatch(subscription -> subscription.token().equals(deviceToken));
    }
}
