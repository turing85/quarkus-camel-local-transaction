package de.turing85.quarkus.camel.local.transactions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.ws.rs.core.Response;

import com.google.common.truth.Truth;
import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.spi.RouteController;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.function.Predicate.not;

import static de.turing85.quarkus.camel.local.transactions.JmsToDbRoute.GLOBAL_STOP_VARIABLE;

@QuarkusTest
class JmsToDbRouteTest {
  public static final String THROWER_ID = "thrower";

  @Inject
  CamelContext camelContext;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  ConnectionFactory connectionFactory;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  AgroalDataSource dataSource;

  private final Random random = new Random();

  @BeforeEach
  void setup() throws Exception {
    stopAllRoutes();
    emptyQueue();
    emptyTable();
    startAllRoutes();
  }

  @Test
  void sendMessage() throws Exception {
    // given
    final int numberToSend = random.nextInt(1_000_000);

    // when
    sendToTopic(numberToSend);

    // then
    assertHealthUp();
    assertDbHasNEntriesForValue(1, numberToSend);
    assertDbHasNEntriesForValue(1, numberToSend + 1);
    assertNoMoreMessagesOnQueue();
  }

  @Test
  void failOnNumberReceiver() throws Exception {
    try {
      // given
      addThrowerToRoute(JmsToDbRoute.NUMBER_RECEIVER_TO_DB);
      final int numberToSend = random.nextInt(1_000_000);

      // when
      sendToTopic(numberToSend);

      // then
      assertHealthDown();
      assertDbHasNEntriesForValue(4, numberToSend);
      assertDbHasNEntriesForValue(4, numberToSend + 1);
      assertMessageOnQueue(numberToSend);
      assertNoMoreMessagesOnQueue();
    } finally {
      // cleanup
      removeThrowerFromRoute(JmsToDbRoute.NUMBER_RECEIVER_TO_DB);
    }
  }

  @Test
  void failOnDbWriter() throws Exception {
    try {
      // given
      addThrowerToRoute(JmsToDbRoute.DB_WRITER);
      final int numberToSend = random.nextInt(1_000_000);

      // when
      sendToTopic(numberToSend);

      // then
      assertHealthDown();
      assertDbHasNEntriesForValue(0, numberToSend);
      assertDbHasNEntriesForValue(0, numberToSend + 1);
      assertMessageOnQueue(numberToSend);
      assertNoMoreMessagesOnQueue();
    } finally {
      // cleanup
      removeThrowerFromRoute(JmsToDbRoute.DB_WRITER);
    }
  }

  private void stopAllRoutes() throws Exception {
    camelContext.getRouteController().stopAllRoutes();
    assertAllRoutesHaveStatus(ServiceStatus.Stopped);
    assertHealthDown();
  }

  private void startAllRoutes() {
    camelContext.setVariable(GLOBAL_STOP_VARIABLE, false);
    // @formatter:off
    RouteController routeController = camelContext.getRouteController();
    camelContext.getRoutes().stream()
        .map(Route::getRouteId)
        .toList().stream()
        .filter(not(routeId -> routeController.getRouteStatus(routeId).isStarted()))
        .forEach(routeId -> {
          try {
            routeController.startRoute(routeId);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    // @formatter:on
    assertAllRoutesHaveStatus(ServiceStatus.Started);
    assertHealthUp();
  }

  private void assertAllRoutesHaveStatus(ServiceStatus status) {
    // @formatter:off
    camelContext.getRoutes().stream()
        .map(Route::getRouteId)
        .forEach(routeId -> Awaitility.await()
            .atMost(Duration.ofSeconds(1))
            .untilAsserted(() -> Truth
                .assertThat(camelContext.getRouteController().getRouteStatus(routeId))
                .isEqualTo(status)));
    // @formatter:on
  }

  private void emptyQueue() throws JMSException {
    try (JMSContext context = connectionFactory.createContext()) {
      Queue queueDestination =
          context.createQueue("%s::%s".formatted(JmsToDbRoute.TOPIC, JmsToDbRoute.QUEUE));
      JMSConsumer consumer = context.createConsumer(queueDestination);
      while (true) {
        Message message = consumer.receive(Duration.ofSeconds(1).toMillis());
        if (Objects.nonNull(message)) {
          message.acknowledge();
        } else {
          break;
        }
      }
    }
  }

  private void emptyTable() throws SQLException {
    try (Statement statement = dataSource.getConnection().createStatement()) {
      statement.execute("TRUNCATE TABLE numbers");
    }
  }

  private void addThrowerToRoute(String routeId) throws Exception {
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeId,
        advice -> advice.weaveAddLast()
            .throwException(new Exception("Exception to test transaction")).id(THROWER_ID));
    // @formatter:on
  }

  private void removeThrowerFromRoute(String routeId) throws Exception {
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeId,
        advice -> advice.weaveById(THROWER_ID).remove());
    // @formatter:on
  }

  private void sendToTopic(int bodyToSend) {
    try (JMSContext context = connectionFactory.createContext()) {
      context.createProducer().send(context.createTopic(JmsToDbRoute.TOPIC), bodyToSend);
    }
  }

  private static void assertHealthUp() {
    // @formatter:off
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> RestAssured
            .when().get("/q/health")
            .then().statusCode(Response.Status.OK.getStatusCode()));
    // @formatter:on
  }

  private static void assertHealthDown() {
    // @formatter:off
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> RestAssured
            .when().get("/q/health")
            .then().statusCode(Response.Status.SERVICE_UNAVAILABLE.getStatusCode()));
    // @formatter:on
  }

  private void assertMessageOnQueue(int expectedBody) throws JMSException {
    try (JMSContext context = connectionFactory.createContext()) {
      Queue queueDestination =
          context.createQueue("%s::%s".formatted(JmsToDbRoute.TOPIC, JmsToDbRoute.QUEUE));
      JMSConsumer consumer = context.createConsumer(queueDestination);
      Message message = consumer.receive(Duration.ofSeconds(1).toMillis());
      Truth.assertThat(message).isNotNull();
      Truth.assertThat(message.getBody(Integer.class)).isEqualTo(expectedBody);
      message.acknowledge();
    }
  }

  private void assertNoMoreMessagesOnQueue() {
    try (JMSContext context = connectionFactory.createContext()) {
      Queue queueDestination =
          context.createQueue("%s::%s".formatted(JmsToDbRoute.TOPIC, JmsToDbRoute.QUEUE));
      JMSConsumer consumer = context.createConsumer(queueDestination);
      Message message = consumer.receive(Duration.ofSeconds(1).toMillis());
      Truth.assertThat(message).isNull();
    }
  }

  private void assertDbHasNEntriesForValue(int n, int value) throws SQLException {
    try (Statement statement = dataSource.getConnection().createStatement()) {
      // @formatter:off
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> {
            ResultSet rs = statement
                .executeQuery("SELECT COUNT(*) FROM numbers WHERE value = %s".formatted(value));
            Truth.assertThat(rs.next()).isTrue();
            Truth.assertThat(rs.getInt(1)).isEqualTo(n);
          });
      // @formatter:on
    }
  }
}
