package de.turing85.quarkus.camel.local.transactions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Random;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
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

  @Test
  void sendMessage() throws Exception {
    // given
    int numberToSend = random.nextInt(1_000_000);

    // when
    sendToQueue("numbers", numberToSend);

    // then
    assertHealthUp();
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
    assertHealthDown();
    assertDbHasNEntriesWithValue(1, numberToSend);

    // given
    stopCamel();
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeToFailOn,
        advice -> advice.weaveById("thrower").remove());
    // @formatter:on

    // when
    startCamel();

    // then
    assertDbHasNEntriesWithValue(2, numberToSend);
  }

  @Test
  void failOnInnerRoute() throws Exception {
    // given
    String routeToFailOn = JmsToDbRoute.DB_WRITER;
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeToFailOn,
        advice -> advice.weaveAddLast()
            .throwException(new Exception("test transaction"))
            .id("thrower"));
    // @formatter:on
    int numberToSend = random.nextInt(1_000_000);
    String queue = "numbers";

    // when
    sendToQueue(queue, numberToSend);

    // then
    assertHealthDown();
    assertDbHasNEntriesWithValue(0, numberToSend);

    // given
    stopCamel();
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeToFailOn,
        advice -> advice.weaveById("thrower").remove());
    // @formatter:on

    // when
    startCamel();

    // then
    assertDbHasNEntriesWithValue(1, numberToSend);
  }

  private void stopCamel() {
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
    for (Route route : camelContext.getRoutes()) {
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> Truth
              .assertThat(camelContext.getRouteController().getRouteStatus(route.getId()))
              .isEqualTo(ServiceStatus.Started));
    }
    // @formatter:on
    assertHealthUp();
  }

  private void assertDbHasNEntriesWithValue(int n, int value) throws SQLException {
    try (Statement statement = dataSource.getConnection().createStatement()) {
      // @formatter:off
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> {
            ResultSet rs = statement
                .executeQuery("SELECT COUNT(*) FROM numbers WHERE value = %s"
                    .formatted(value));
            Truth.assertThat(rs.next()).isTrue();
            Truth.assertThat(rs.getInt(1)).isEqualTo(n);
          });
      // @formatter:on
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

  private void sendToQueue(String queue, int numberToSend) {
    try (JMSContext context = connectionFactory.createContext()) {
      context.createProducer().send(context.createQueue(queue), numberToSend);
    }
  }
}
