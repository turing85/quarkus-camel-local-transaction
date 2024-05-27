package de.turing85.quarkus.camel.local.transactions;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.ConnectionFactory;

import io.agroal.api.AgroalDataSource;
import lombok.AllArgsConstructor;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.*;

@ApplicationScoped
@AllArgsConstructor
public class JmsToDbRoute extends RouteBuilder {
  private final ConnectionFactory connectionFactory;
  private final AgroalDataSource dataSource;

  @Override
  public void configure() {
    // @formatter:off
    from(
        timer("sender")
            .delay(Duration.ofSeconds(1).toMillis())
            .period(Duration.ofSeconds(1).toMillis())
            .includeMetadata(true))
        .routeId("timer-number-sender")
        .setBody(exchangeProperty(Exchange.TIMER_COUNTER))
        .log("Sending ${body}")
        .to(jms("numbers")
            .connectionFactory(connectionFactory));

    from(
        jms("numbers")
            .connectionFactory(connectionFactory)
            .transacted(true))
        .routeId("number-receiver-to-db")
        .to(direct("db-writer"));

    from(direct("db-writer"))
        .routeId("db-writer")
        .transacted("PROPAGATION_REQUIRES_NEW")
        .convertBodyTo(int.class)
        .log("Receiving ${body}")
        .to(sql("INSERT INTO numbers(value) VALUES(:#${body})")
            .dataSource(dataSource));
    // @formatter:on
  }
}
