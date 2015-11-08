package com.mhack.parallel.vertx;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import io.vertx.core.TimeoutStream;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;

public class HttpResponseAggregation {
  private static final Logger logger = LoggerFactory.getLogger(HttpResponseAggregation.class);
  private static final Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
  private final HttpServerResponse response;
  private final Vertx vertx;
  private final Map<String, String> depStatus;
  private final Map<String, Buffer> depResults;
  private int depsRemaining;
  private final TimeoutStream timeout;
  
  public HttpResponseAggregation(Vertx vertx, HttpServerResponse response, int globalTimeout, String ... deps) {
    this.vertx = vertx;
    this.response = response;
    depResults = new HashMap<>();
    depStatus = new HashMap<>(deps.length);
    for (String d : deps) {
      depStatus.put(d, "pending");
    }
    depsRemaining = depStatus.size();
    timeout = setTimeout(globalTimeout);
  }

  public void end(String depName, Buffer body) {
    depStatus.put(depName, "completed");
    depResults.put(depName, body);
    updatedRemaining();
  }
  
  public void end(String depName, Throwable err) {
    depStatus.put(depName, "error");
    depResults.put(depName, errorAsJson(err));
    updatedRemaining();
  }
  
  private Buffer errorAsJson(Throwable err) {
    return Buffer.buffer(String.format("{\"error\":\"%s\"}", err.getMessage()));
  }

  private void updatedRemaining() {
    depsRemaining--;
    if (depsRemaining == 0) {
      endResponse();
      timeout.cancel();
    }
  }
  
  private void endResponse() {
    response.end(aggResultString());
  }
  
  // TODO transform the aggResult in a object to avoid this JSON massaging
  private String aggResultString() {
    StringBuilder sb = new StringBuilder("{");
    for (String key : depStatus.keySet()) {
      sb.append("\"").append(key).append("\":{");
      sb.append("\"status\":\"").append(depStatus.get(key)).append("\"");
      if (depResults.containsKey(key)) {
        sb.append(",\"result\":").append(depResults.get(key));
      }
      sb.append("},");
    }
    sb.append("\"foo\":\"bar\"}"); // jq complains about the trailing ',' :)
    return sb.toString();
  }
  
  private TimeoutStream setTimeout(int globalTimeout) {
    return vertx.timerStream(globalTimeout).handler(t -> {
      if (depsRemaining == 0) {
        return;
      }
      for (Map.Entry<String, String> item : depStatus.entrySet()) {
        if (item.getValue().equals("pending")) {
          item.setValue("timeout");
        }
      }
      logger.warn("Timeout. {} dep(s) remaining: {}", depsRemaining, mapJoiner.join(depStatus));
      endResponse();
    });
  }
}