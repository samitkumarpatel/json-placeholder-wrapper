package net.samitkumar.json_placeholder_wrapper;

import io.netty.channel.ChannelOption;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.support.WebClientAdapter;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.List;

@SpringBootApplication
@Slf4j
public class JsonPlaceholderWrapperApplication {

	public static void main(String[] args) {
		SpringApplication.run(JsonPlaceholderWrapperApplication.class, args);
	}

	@Bean
	public JsonPlaceHolderClient jsonPlaceHolderClient(WebClient.Builder clientBuilder) {

		var builder = clientBuilder
				.baseUrl("https://jsonplaceholder.typicode.com/")
				.filter(ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
					var statusCode = clientResponse.statusCode();
					var uri = clientResponse.request().getURI();
					var method = clientResponse.request().getMethod();
					log.info("method: {}, uri: {}, status: {}", method, uri, statusCode);
					return Mono.just(clientResponse);
				}))
				.clientConnector(new ReactorClientHttpConnector(
						HttpClient
								.create()
								.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
								.responseTimeout(Duration.ofSeconds(5))
						)
				)
				.build();
		WebClientAdapter adapter = WebClientAdapter.create(builder);
		HttpServiceProxyFactory factory = HttpServiceProxyFactory.builderFor(adapter).build();
		return factory.createClient(JsonPlaceHolderClient.class);
	}

	@Bean
	RouterFunction<ServerResponse> routes(CacheService cacheService) {
		return RouterFunctions
				.route()
				.GET("/users", request -> cacheService.getCachedUsers().flatMap(ServerResponse.ok()::bodyValue))
				.build();
	}

}

record User(int id, String name, String username, String email) {}

@HttpExchange
interface JsonPlaceHolderClient {
	@GetExchange("/users")
	Mono<List<User>> getUsers();
}

@Service
@Slf4j
@RequiredArgsConstructor
class UserService {
	private final JsonPlaceHolderClient client;

	public Mono<List<User>> getUsers() {
		log.info("### UserService::getUsers()");
		return client
				.getUsers()
				.doOnSuccess(users -> log.info("UserService::getUsers() SUCCESS with size {}", users.size()))
				.doOnError(error -> log.error("UserService::getUsers() FAILED", error));
	}
}

@Service
@RequiredArgsConstructor
@Slf4j
class CacheService {
	private final UserService userService;

	public Mono<List<User>> getCachedUsers() {
		return cachedUsers()
				.next();
	}

	private Mono<List<User>> allUsers() {
		log.info("#### CacheService::allUsers()");
		return userService
				.getUsers();
	}

	private Flux<List<User>> cachedUsers() {
		return Mono.just("repeatable")
				.flatMap(s -> allUsers())
				.cache(
						listOfUsers -> Duration.ofMinutes(1L),
						(throwable -> Duration.ZERO),
						() -> Duration.ZERO
				)
				.repeat();
	}
}
