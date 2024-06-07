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
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.AdviceWith;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class JmsToDbRouteTest {
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
  void setup() throws JMSException, SQLException {
    emptyQueue();
    emptyTable();
  }

  @Test
  void sendMessage() throws Exception {
    // given
    int numberToSend = random.nextInt(1_000_000);

    // when
    sendToTopic(numberToSend);

    // then
    assertHealthUp();
    assertDbHasNEntriesWithValue(1, numberToSend);
    assertDbHasNEntriesWithValue(1, numberToSend + 1);
    assertNoMoreMessagesOnQueue();
  }

  @Test
  void failOnOuterRoute() throws Exception {
    // given
    addThrowerToRoute(JmsToDbRoute.NUMBER_RECEIVER_TO_DB);
    final int numberToSend = random.nextInt(1_000_000);

    // when
    sendToTopic(numberToSend);

    // then
    assertHealthDown();
    assertDbHasNEntriesWithValue(1, numberToSend);
    assertDbHasNEntriesWithValue(1, numberToSend + 1);
    assertMessageOnQueue(numberToSend);
    assertNoMoreMessagesOnQueue();

    // cleanup
    removeThrowerFromRoute(JmsToDbRoute.NUMBER_RECEIVER_TO_DB);
  }

  @Test
  void failOnInnerRoute() throws Exception {
    // given
    addThrowerToRoute(JmsToDbRoute.DB_WRITER);
    final int numberToSend = random.nextInt(1_000_000);

    // when
    sendToTopic(numberToSend);

    // then
    assertHealthDown();
    assertDbHasNEntriesWithValue(0, numberToSend);
    assertDbHasNEntriesWithValue(0, numberToSend + 1);
    assertMessageOnQueue(numberToSend);
    assertNoMoreMessagesOnQueue();

    // cleanup
    removeThrowerFromRoute(JmsToDbRoute.DB_WRITER);
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

  private void addThrowerToRoute(String routeToFailOn) throws Exception {
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeToFailOn,
        advice -> advice.weaveAddLast()
            .throwException(new Exception("test transaction")).id("thrower"));
    // @formatter:on
  }

  private void sendToTopic(int numberToSend) {
    try (JMSContext context = connectionFactory.createContext()) {
      context.createProducer().send(context.createTopic(JmsToDbRoute.TOPIC), numberToSend);
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
            .then()
            .statusCode(Response.Status.SERVICE_UNAVAILABLE.getStatusCode()));
    // @formatter:on
  }

  private void assertMessageOnQueue(int numberToSend) throws JMSException {
    try (JMSContext context = connectionFactory.createContext()) {
      Queue queueDestination =
          context.createQueue("%s::%s".formatted(JmsToDbRoute.TOPIC, JmsToDbRoute.QUEUE));
      JMSConsumer consumer = context.createConsumer(queueDestination);
      Message message = consumer.receive(Duration.ofSeconds(1).toMillis());
      Truth.assertThat(message).isNotNull();
      Truth.assertThat(message.getBody(Integer.class)).isEqualTo(numberToSend);
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

  private void assertDbHasNEntriesWithValue(int n, int value) throws SQLException {
    try (Statement statement = dataSource.getConnection().createStatement()) {
      // @formatter:off
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> {
            ResultSet rs = statement
                .executeQuery(
                    "SELECT COUNT(*) FROM numbers WHERE value = %s".formatted(value));
            Truth.assertThat(rs.next()).isTrue();
            Truth.assertThat(rs.getInt(1)).isEqualTo(n);
          });
      // @formatter:on
    }
  }

  private void removeThrowerFromRoute(String routeToFailOn) throws Exception {
    suspendCamel();
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeToFailOn,
        advice -> advice.weaveById("thrower").remove());
    // @formatter:on
    startCamel();
  }

  private void suspendCamel() {
    camelContext.suspend();
    // @formatter:off
    Awaitility.await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(() ->
            Truth.assertThat(camelContext.isSuspended()).isTrue());
    // @formatter:on
  }

  private void startCamel() throws Exception {
    camelContext.getRouteController().reloadAllRoutes();
    camelContext.start();
    // @formatter:off
    Awaitility.await()
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(() -> Truth.assertThat(camelContext.isStarted()).isTrue());
    camelContext.getRoutes().forEach(route ->
        Awaitility.await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> Truth
                .assertThat(camelContext.getRouteController().getRouteStatus(route.getId()))
                .isEqualTo(ServiceStatus.Started)));
    // @formatter:on
    assertHealthUp();
  }
}
