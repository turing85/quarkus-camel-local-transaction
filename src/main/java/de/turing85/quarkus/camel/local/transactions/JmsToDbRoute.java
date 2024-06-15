package de.turing85.quarkus.camel.local.transactions;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.ConnectionFactory;

import io.agroal.api.AgroalDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.context.ManagedExecutor;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.jms;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.sql;

@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class JmsToDbRoute extends RouteBuilder {
  public static final String NUMBER_RECEIVER_TO_DB = "number-receiver-to-db";
  public static final String DB_WRITER = "db-writer";
  public static final String TOPIC = "numbers";
  public static final String QUEUE = "numbers-to-db";
  public static final String GLOBAL_STOP_VARIABLE = "global:stop";

  private final ConnectionFactory connectionFactory;
  private final AgroalDataSource dataSource;
  private final ManagedExecutor executor;

  @Override
  public void configure() {
    // @formatter:off
    onException(Exception.class)
        .maximumRedeliveries(3)
        .log("Caught: ${exchangeProperty.%s}".formatted(Exchange.EXCEPTION_CAUGHT))
        .process(exchange -> {
          log.info("Rolling back transaction");
          exchange.setRollbackOnly(true);
          exchange.getContext().setVariable(GLOBAL_STOP_VARIABLE, true);
        })
        .rollback()
        .onRedelivery(exchange -> log.info(
            "Redelivery {} / {}",
            exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER),
            exchange.getIn().getHeader(Exchange.REDELIVERY_MAX_COUNTER)));

    interceptFrom("jms:*")
        .when(variable(GLOBAL_STOP_VARIABLE).isEqualTo(true))
            .process(exchange -> {
              log.info("stopping");
              exchange.setVariable(GLOBAL_STOP_VARIABLE, false);
              exchange.setRollbackOnly(true);
              executor.submit(() -> {
                  exchange.getContext().getRouteController().stopAllRoutes();
                  return null;
              });
            });

    from(
        jms("topic:%s".formatted(TOPIC))
            .connectionFactory(connectionFactory)
            .subscriptionShared(true)
            .subscriptionDurable(true)
            .durableSubscriptionName(QUEUE)
            .transacted(true))
        .transacted()
        .routeId(NUMBER_RECEIVER_TO_DB)
        .log("Receiving ${body}")
        .to(direct(DB_WRITER));

    from(direct(DB_WRITER))
        .routeId(DB_WRITER)
        .transacted("PROPAGATION_REQUIRES_NEW")
        .convertBodyTo(int.class)
        .log("Receiving ${body}")
        .to(sql("INSERT INTO numbers(value) VALUES(:#${body})")
            .dataSource(dataSource))
        .process(exchange -> exchange.getIn().setBody(exchange.getIn().getBody(Integer.class) + 1))
        .to(sql("INSERT INTO numbers(value) VALUES(:#${body})")
            .dataSource(dataSource));
    // @formatter:on
  }
}
