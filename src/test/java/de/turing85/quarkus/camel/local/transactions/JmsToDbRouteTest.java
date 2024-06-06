package de.turing85.quarkus.camel.local.transactions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Random;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.ws.rs.core.Response;

import com.google.common.truth.Truth;
import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
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
  void setUp() throws SQLException, JMSException {
    try (JMSContext context = connectionFactory.createContext()) {
      JMSConsumer consumer = context.createConsumer(context.createQueue("numbers"));
      while (true) {
        Message message = consumer.receive(Duration.ofSeconds(1).toMillis());
        if (message == null) {
          break;
        } else {
          message.acknowledge();
        }
      }
    }
    try (Statement statement = dataSource.getConnection().createStatement()) {
      statement.execute("TRUNCATE TABLE numbers;");
    }
  }

  @Test
  void sendMessage() throws Exception {
    // given
    int numberToSend = random.nextInt(1_000_000);

    // when
    sendToQueue("numbers", numberToSend);

    // then
    awaitHealthUp();
    assertDbHasNEntriesWithValue(1, numberToSend);
  }

  @Test
  void failOnOuterRoute() throws Exception {
    // given
    String routeToFailOn = JmsToDbRoute.NUMBER_RECEIVER_TO_DB;
    AdviceWith.adviceWith(camelContext, routeToFailOn, advice -> advice.weaveAddLast()
        .throwException(new Exception("test transaction")).id("thrower"));
    int numberToSend = random.nextInt(1_000_000);
    String queue = "numbers";

    // when
    sendToQueue(queue, numberToSend);

    // then
    awaitHealthDown();

    assertDbHasNEntriesWithValue(1, numberToSend);

    stopCamel();
    AdviceWith.adviceWith(camelContext, routeToFailOn,
        advice -> advice.weaveById("thrower").remove());
    startCamel();
    try (JMSContext context = connectionFactory.createContext()) {
      JMSConsumer consumer = context.createConsumer(context.createQueue(queue));
      Truth.assertThat(consumer.receive(Duration.ofSeconds(1).toMillis())).isNull();
    }
    assertDbHasNEntriesWithValue(2, numberToSend);
  }

  @Test
  void failOnInnerRoute() throws Exception {
    // given
    String routeToFailOn = JmsToDbRoute.DB_WRITER;
    AdviceWith.adviceWith(camelContext, routeToFailOn, advice -> advice.weaveAddLast()
        .throwException(new Exception("test transaction")).id("thrower"));
    int numberToSend = random.nextInt(1_000_000);
    String queue = "numbers";

    // when
    sendToQueue(queue, numberToSend);

    // then
    awaitHealthDown();

    assertDbHasNEntriesWithValue(0, numberToSend);

    stopCamel();
    AdviceWith.adviceWith(camelContext, routeToFailOn,
        advice -> advice.weaveById("thrower").remove());
    startCamel();
    try (JMSContext context = connectionFactory.createContext()) {
      JMSConsumer consumer = context.createConsumer(context.createQueue(queue));
      Truth.assertThat(consumer.receive(Duration.ofSeconds(1).toMillis())).isNull();
    }
    assertDbHasNEntriesWithValue(1, numberToSend);
  }

  private void stopCamel() {
    camelContext.suspend();
    Awaitility.await().atMost(Duration.ofMinutes(1))
        .untilAsserted(() -> Truth.assertThat(camelContext.isSuspended()).isTrue());
  }

  private void startCamel() throws Exception {
    camelContext.getRouteController().reloadAllRoutes();
    camelContext.start();
    Awaitility.await().atMost(Duration.ofMinutes(1))
        .untilAsserted(() -> Truth.assertThat(camelContext.isStarted()).isTrue());
    for (Route route : camelContext.getRoutes()) {
      Awaitility.await().atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> Truth
              .assertThat(camelContext.getRouteController().getRouteStatus(route.getId()))
              .isEqualTo(ServiceStatus.Started));
    }
    awaitHealthUp();
  }

  private void assertDbHasNEntriesWithValue(int n, int value) throws SQLException {
    try (Statement statement = dataSource.getConnection().createStatement()) {
      Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
        ResultSet rs = statement
            .executeQuery("SELECT COUNT(*) FROM numbers WHERE value = %s".formatted(value));
        Truth.assertThat(rs.next()).isTrue();
        Truth.assertThat(rs.getInt(1)).isEqualTo(n);
      });
    }
  }

  private static void awaitHealthUp() {
    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> RestAssured.when()
        .get("/q/health").then().statusCode(Response.Status.OK.getStatusCode()));
  }

  private static void awaitHealthDown() {
    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> RestAssured.when()
        .get("/q/health").then().statusCode(Response.Status.SERVICE_UNAVAILABLE.getStatusCode()));
  }

  private void sendToQueue(String queue, int numberToSend) {
    try (JMSContext context = connectionFactory.createContext()) {
      context.createProducer().send(context.createQueue(queue), numberToSend);
    }
  }
}
