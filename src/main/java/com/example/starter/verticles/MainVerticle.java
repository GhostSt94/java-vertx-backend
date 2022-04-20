package com.example.starter.verticles;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class MainVerticle extends AbstractVerticle {


  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    EventBus eb= vertx.eventBus();
    Router router=Router.router(vertx);

    SockJSBridgeOptions options = new SockJSBridgeOptions();
    PermittedOptions address = new PermittedOptions().setAddressRegex("[^\n]+");
    options.addInboundPermitted(address);
    options.addOutboundPermitted(address);

    SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
    router.mountSubRouter("/eventbus",sockJSHandler.bridge(options, be -> {
      if (be.type() == BridgeEventType.REGISTER) {
        System.out.println("sockJs: connected");
      }
      be.complete(true);
    }));

    router.route().handler(BodyHandler.create());
    router.route().handler(CorsHandler.create("http://localhost:8080")
      .allowedMethod(HttpMethod.POST)
      .allowedMethod(HttpMethod.GET));


    router.post("/file/save").handler(ctx->{

      FileSystem fs= vertx.fileSystem();
      Set<FileUpload> fileUploadSet = ctx.fileUploads();
      //if file exists
      if (fileUploadSet == null || fileUploadSet.isEmpty()) {
        System.out.println("no file found");
      }else{
        for (FileUpload f : fileUploadSet) {
          vertx.eventBus().request("com.client",new JsonObject().put("message","File uploaded"));
          fs.readDir("file-uploads/",ls -> {
            String filePath=ls.result().get(0);
            //read file uploaded
            fs.readFile(filePath,bufferAsyncResult -> {
              //FileName
              String fName=f.fileName().split(".csv")[0];
              String fileName =fName+"_"+new SimpleDateFormat("yyyyMMddHHmm'.csv'").format(new Date());
              Buffer buff=bufferAsyncResult.result();
              JsonObject obj=new JsonObject()
                .put("fName",fileName)
                .put("buff",buff);
              //WriteFile
              Future<Void> futWriteFile=fs.writeFile("src/main/resources/uploads/"+fileName, buff);
              //Save file to mongodb
              JsonObject data=new JsonObject()
                .put("fName",fileName)
                .put("buff",buff);
              Future<Message<Object>> futSaveFile= vertx.eventBus().request("file.save.file",data);

              CompositeFuture.all(futWriteFile,futSaveFile).onComplete(ar -> {
                if (ar.succeeded()){
                  ctx.response().setStatusCode(200).end(futSaveFile.result().body().toString());
                  //eb.send("com.client",new JsonObject().put("message","File saved in database"));

                  getFilesList();
                }else{
                  System.out.println("failed "+ar.cause());
                }
              });
              /*resp->{
                if(resp.succeeded()){
                  ctx.response().setStatusCode(200).end((String) resp.result().body());
                }
              });*/
            });
            fs.delete(filePath);
          });
        }
      }
    });

    eb.consumer("list.files",res->{
      eb.request("list.files.all","",resp -> {
        if(resp.succeeded()){
          JsonObject obj= (JsonObject) resp.result().body();
          res.reply(obj);
        }
      });
    });
    eb.consumer("select.file",res->{
      getFileDocs(res.body().toString());
        /*eb.request("file.docs.all", res.body(), resp -> {
          if (resp.succeeded()) {
            JsonArray arr = (JsonArray) resp.result().body();
            eb.request("file.docs",arr);
            res.reply(arr);
          }
        });*/
    });
    eb.consumer("get.doc",res->{
        eb.request("get.doc.id", res.body(), resp -> {
          if (resp.succeeded()) {
            JsonObject doc = (JsonObject) resp.result().body();
            eb.send("show.doc",doc);
            res.reply(doc);
          }
        });
    });
    eb.consumer("update.doc.one",res->{
      eb.request("update.doc.db",res.body(),resp->{
        if(resp.succeeded()){
          JsonObject obj=(JsonObject)res.body();
          String file_name= obj.getString("file_name");
          /*eb.request("file.docs.all", fileName, resp2 -> {
            if (resp2.succeeded()) {
              JsonArray arr = (JsonArray) resp2.result().body();
              eb.request("file.docs",arr);
              res.reply(arr);
            }
          });*/
          getFileDocs(file_name);
          res.reply("updated");
        }else{
          res.fail(404,"Error updating");
        }
      });
    });
    eb.consumer("delete.doc",res->{
        JsonObject obj = (JsonObject) res.body();
        String id=obj.getString("id");
        String file_name=obj.getString("file_name");
        eb.request("delete.doc.id", id, resp -> {
          if (resp.failed()) {
            res.fail(400,"Error");
          }else{
            /*eb.request("file.docs.all", obj.getString("file_name"), resp2 -> {
              if (resp2.succeeded()) {
                JsonArray arr = (JsonArray) resp2.result().body();
                eb.request("file.docs",arr);
                res.reply(arr);
              }
            });*/
            getFileDocs(file_name);
            res.reply("deleted "+resp.result().body());
          }
        });
    });

    vertx.createHttpServer().requestHandler(router)
      .listen(8888, http -> {
        if (http.succeeded()) {
          startPromise.complete();
          System.out.println("HTTP server started on port 8888");
        } else {
          startPromise.fail(http.cause());
        }
      });
  }


  public void getFilesList(){
    vertx.eventBus().request("list.files.all","",resp -> {
      if(resp.succeeded()){
        JsonObject obj_files= (JsonObject) resp.result().body();
        vertx.eventBus().request("list.files.update",obj_files);
        System.out.println("updated list");
      }
    });
  }
  public void getFileDocs(String fileName){
    vertx.eventBus().request("file.docs.all", fileName, resp2 -> {
      if (resp2.succeeded()) {
        JsonArray arr = (JsonArray) resp2.result().body();
        vertx.eventBus().request("file.docs",arr);
        //res.reply(arr);
      }
    });
  }
}
