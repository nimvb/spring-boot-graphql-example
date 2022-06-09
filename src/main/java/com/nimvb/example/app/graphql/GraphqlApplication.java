package com.nimvb.example.app.graphql;

import com.github.javafaker.Faker;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.graphql.execution.RuntimeWiringConfigurer;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class GraphqlApplication {

    public static void main(String[] args) {
        SpringApplication.run(GraphqlApplication.class, args);
    }

    @Bean
    Faker faker() {
        return new Faker();
    }


    @Bean
    ApplicationRunner runner(UserService userService, NotificationService notificationService, TaskScheduler scheduler, Faker faker) {
        return args -> {
            scheduler.schedule(() -> {
                Instant now = Instant.now();
                log.info("scheduler executes at {}", now);
                String email = faker.name().username() + "@test.com";
                String password = faker.crypto().md5();
                try {
                    User user = new User();
                    user.setEmail(email);
                    user.setPassword(password);
                    userService.create(user);
                    notificationService.notify(user);
                    log.info("user created {} at {}", user, now);
                } catch (Exception ex) {
                    log.error("user creation failed for {}!", email);
                }
            }, new CronTrigger("0/5 * * * * * "));
        };
    }


    @Bean
    RuntimeWiringConfigurer runtimeWiring(UserService userService, SubscriptionService subscriptionService) {
        return builder -> {
            builder.type("Query", wb -> wb.dataFetcher("message", environment -> "Hello World!"));
            builder.type("Query", wb -> wb.dataFetcher("user", environment -> {
                String email = environment.getArgumentOrDefault("email", null);
                return userService.find(email);
            }));
            builder.type("Query", wb -> wb.dataFetcher("users", environment -> userService.users()));
            builder.type("Subscription", wb -> wb.dataFetcher("users", environment -> subscriptionService.subscription()));
            builder.type("Subscription", wb -> wb.dataFetcher("current", environment -> userService.users()));
        };
    }


    @Data
    public static class User {
        private String email;
        private String password;
    }

    @Service
    @Slf4j
    public static class NotificationService {
        private final Sinks.Many<User> sink = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE,false);

        public void notify(User user){
            sink.emitNext(user,(signalType, emitResult) -> {
                log.error("event emission failed {}, {}",signalType,emitResult);
                return false;
            });
        }

        public Sinks.Many<User> sink(){
            return sink;
        }
    }

    @Service
    @Slf4j
    public static class SubscriptionService {

        private final NotificationService notificationService;
        private final Flux<User> userSubscription;


        public SubscriptionService(@NonNull NotificationService notificationService) {
            this.notificationService = notificationService;
            this.userSubscription = notificationService.sink().asFlux();
        }

        public Flux<User> subscription(){
            return userSubscription;
        }
    }

    @Service
    public static class UserService {

        public Map<String, User> users = new HashMap<>();

        public void create(User user) {
            synchronized (this) {
                Objects.requireNonNull(user);
                Objects.requireNonNull(user.email);
                if (exists(user.email)) {
                    throw new RuntimeException();
                }
                users.put(user.email, user);
            }
        }

        public boolean exists(String email) {
            return users.containsKey(email);
        }

        public Flux<User> users() {
            return Flux.fromStream(users.values().stream());
        }

        public Mono<User> find(String email) {
            return Mono.justOrEmpty(Optional.ofNullable(users.getOrDefault(email, null)));
        }

    }
}
