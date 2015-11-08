package com.mhack.parallel.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;

public class OrdersService {
  private final Vertx vertx;
  private final int globalTimeout;
  private final HttpClient client;
  private final Router router;

  public static void main(String[] args) {
    final int GLOBAL_TIMEOUT = 1000;
    final Vertx vertx = Vertx.vertx();
    OrdersService orderService = new OrdersService(vertx, GLOBAL_TIMEOUT).start();
    vertx.createHttpServer().requestHandler(orderService.router()::accept).listen(8080);
  }
  
  public OrdersService(Vertx vertx, int globalTimeout) {
    this.vertx = vertx;
    this.globalTimeout = globalTimeout;
    client = vertx.createHttpClient(new HttpClientOptions()
        .setConnectTimeout(globalTimeout)
        .setMaxPoolSize(1000));
    router = Router.router(vertx);
  }
  
  public OrdersService start() {
    setupRoutes();
    return this;
  }
  
  public Router router() {
    return this.router;
  }
  
  private void setupRoutes() {
    router.get("/order/:id").produces("application/json").handler(rc -> {
      final HttpServerResponse res = rc.response().setChunked(true);
      final String orderId = rc.request().getParam("id");
      final HttpResponseAggregation agg = new HttpResponseAggregation(vertx, res, globalTimeout, "order", "inventory", "payment", "risk");
      res.putHeader("Content-Type", "application/json");
      get("order"    , 9292, "localhost", "/oms/orders/"+orderId             , agg);
      get("inventory", 9292, "localhost", "/ims/reserves?orderId="+orderId   , agg);
      get("payment"  , 9292, "localhost", "/payment/charges?orderId="+orderId, agg);
      get("risk"     , 9292, "localhost", "/risk/checks?orderId="+orderId    , agg);
    });
    
  }

  private void get(String depName, int port, String host, String URI, HttpResponseAggregation agg) {
    client.get(port, host, URI, res -> {
      res.bodyHandler(body -> {
        agg.end(depName, body);
      });
      res.exceptionHandler(err -> {
        agg.end(depName, err);
      });
    }).setTimeout(globalTimeout).end();
  }
}
