package de.turing85.quarkus.camel.local.transactions;

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
  public static final String NUMBER_RECEIVER_TO_DB = "number-receiver-to-db";
  public static final String DB_WRITER = "db-writer";

  private final ConnectionFactory connectionFactory;
  private final AgroalDataSource dataSource;

  @Override
  public void configure() {
    // @formatter:off
    onException(Exception.class)
        .log("Caught: ${exchangeProperty.%s}, stopping".formatted(Exchange.EXCEPTION_CAUGHT))
        .process(exchange ->
            new Thread(() -> exchange.getContext().suspend())
                .start())
        .handled(false);
    from(
        jms("numbers")
            .connectionFactory(connectionFactory)
            .transacted(true))
        .routeId(NUMBER_RECEIVER_TO_DB)
        .to(direct(DB_WRITER));

    from(direct(DB_WRITER))
        .routeId(DB_WRITER)
        .transacted("PROPAGATION_REQUIRES_NEW")
        .convertBodyTo(int.class)
        .log("Receiving ${body}")
        .to(sql("INSERT INTO numbers(value) VALUES(:#${body})")
            .dataSource(dataSource));
    // @formatter:on
  }
}
