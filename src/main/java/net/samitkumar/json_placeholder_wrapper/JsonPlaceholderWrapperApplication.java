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
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
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
	JsonPlaceHolderClient jsonPlaceHolderClient(WebClient.Builder clientBuilder) {

		var builder = clientBuilder
				//.baseUrl("https://jsonplaceholder.typicode.com/")
				.baseUrl("http://localhost:3000")
				.filter(ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
					var statusCode = clientResponse.statusCode();
					var uri = clientResponse.request().getURI();
					var method = clientResponse.request().getMethod();
					log.info("{} {} {}", method, uri, statusCode);
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
	RouterFunction<ServerResponse> routes(Flux<List<User>> cachedUsers) {
		return RouterFunctions
				.route()
				.GET("/users", request -> cachedUsers.next()
						.flatMap(ServerResponse.ok()::bodyValue))
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
@RequiredArgsConstructor
@Slf4j
class CacheService {
	private final JsonPlaceHolderClient jsonPlaceHolderClient;
	// This is to use the Old success data if the new data fetch fails.
	Sinks.Many<List<User>> sink = Sinks.many().replay().latest();

	//make sure to create a bean of this method and use it.
	@Bean
	Flux<List<User>> cachedUsers() {
		return Mono.just("repeatable")
				.flatMap(s -> jsonPlaceHolderClient.getUsers())
				.doOnSuccess(users -> {
					log.info("Users fetched SUCCESSFULLY");
					//TODO not useful yet, but can be useful if we want to use the old data if the new data fetch fails.
					sink.tryEmitNext(users);
				})
				.doOnError(error -> log.error("Users fetch FAILED", error))
				.cache(
						listOfUsers -> Duration.ofMinutes(1),
						(Throwable t )-> Duration.ZERO,
						() -> Duration.ZERO
				)
				//TODO find a way to pick the previous data from sink if the new data fetch fails.
				.repeat();

	}
}
