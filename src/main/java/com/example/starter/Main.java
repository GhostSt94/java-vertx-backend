package com.example.starter;

import com.example.starter.verticles.FileService;
import com.example.starter.verticles.MainVerticle;
import io.vertx.core.Vertx;

public class Main {

  public static void main(String[] args){
    Vertx vertx=Vertx.vertx();
    vertx.deployVerticle(new MainVerticle(), res -> {
      vertx.deployVerticle(new FileService());
    });

    //vertx.deployVerticle(new FutureVerticle());
  }
}
