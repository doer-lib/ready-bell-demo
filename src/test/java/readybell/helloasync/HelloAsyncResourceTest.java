package readybell.helloasync;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class HelloAsyncResourceTest {

    @Test
    void createsCompletesAndIsIdempotent() throws InterruptedException {
        UUID uuid = UUID.randomUUID();
        String body = """
                {"input": {"name": "Bob", "dbg-delay-sec": 1}}
                """;

        given().contentType("application/json").body(body)
                .when().post("/hello-async/{uuid}", uuid)
                .then().statusCode(201)
                .header("Location", containsString("/hello-async/" + uuid))
                .body("status", equalTo("IN_PROGRESS"));

        given().when().get("/hello-async/{uuid}", uuid)
                .then().statusCode(200)
                .body("status", equalTo("IN_PROGRESS"))
                .body("input.name", equalTo("Bob"));

        Thread.sleep(1500);

        given().when().get("/hello-async/{uuid}", uuid)
                .then().statusCode(200)
                .body("status", equalTo("READY"))
                .body("output.greeting", equalTo("Hello, Bob!"))
                .body("error_message", nullValue());

        given().contentType("application/json").body(body)
                .when().post("/hello-async/{uuid}", uuid)
                .then().statusCode(201)
                .body("status", equalTo("READY"));

        String differentBody = """
                {"input": {"name": "Alice", "dbg-delay-sec": 1}}
                """;
        given().contentType("application/json").body(differentBody)
                .when().post("/hello-async/{uuid}", uuid)
                .then().statusCode(409);
    }

    @Test
    void badUuidReturnsNotFound() {
        given().when().get("/hello-async/{uuid}", "not-a-uuid")
                .then().statusCode(404);
    }
}
