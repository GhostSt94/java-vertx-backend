package com.example.starter.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class FileService extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    JsonObject config=new JsonObject()
      .put("db_name","testDB")
      .put("connection_string","mongodb://localhost");
    MongoClient client = MongoClient.create(vertx, config);

    EventBus eb=vertx.eventBus();

    eb.consumer("file.save.file",msg->{
      JsonObject obj= (JsonObject) msg.body();

      String fileName=obj.getString("fName");
      Buffer buff=obj.getBuffer("buff");

      String body=buff.toString().replace("\"","");
      String lines[]=body.split("\r\n");
      String fields[]=lines[0].split(",");

      JsonArray data=new JsonArray();
      for (int i = 1; i < lines.length; i++) {
        JsonObject line_data=new JsonObject().put("file_name",fileName);
        int j=0;
        while (j<fields.length){
          line_data.put(fields[j],lines[i].split(",")[j]);
          j++;
        }
        data.add(i-1,line_data);
      }
      //System.out.println(data.encode());

        for (Object coll : data) {
          client.insert("files", (JsonObject) coll,res->{

          });
        }
        //System.out.println(data.size()+" row inserted");
        msg.reply(data.size()+" row inserted");

    });
    eb.consumer("list.files.all",msg->{
      /*client.distinct("files","file_name","file_name",res->{
        if (res.succeeded()){
          System.out.println(res.result().encode());
        }else {
          System.out.println("failed");
        }
      });*/
      JsonObject fields=new JsonObject().put("file_name",true).put("_id",false);
      client.findWithOptions("files",new JsonObject(),new FindOptions().setFields(fields), listAsyncResult -> {
        ArrayList<String> arr=new ArrayList<>();
        listAsyncResult.result().stream().forEach((obj)->{
          arr.add(obj.getString("file_name"));
        });
        Set<String> set=new HashSet<>(arr);
        arr.clear();
        arr.addAll(set);

        msg.reply(new JsonObject().put("list",arr));
      });
    });
    eb.consumer("file.docs.all",msg -> {
      String file_name=msg.body().toString();
      JsonObject query=new JsonObject().put("file_name",file_name);
      client.find("files",query,res->{
        if(res.succeeded()){
          JsonArray arr=new JsonArray();
          res.result().stream().forEach(obj->{
            arr.add(obj);
          });
          msg.reply(arr);
        }
      });
    });
    eb.consumer("get.doc.id",msg -> {
      String id=msg.body().toString();
      JsonObject query=new JsonObject().put("_id",id);
      client.findOne("files",query,null,res->{
        if(res.succeeded()){
          msg.reply(res.result());
        }
      });
    });
    eb.consumer("update.doc.db",msg -> {
      JsonObject data= (JsonObject) msg.body();
      JsonObject doc=new JsonObject().put("$set",data);
      JsonObject query=new JsonObject().put("_id",data.getString("_id"));
      client.findOneAndUpdate("files",query,doc,res->{
        if(res.succeeded()){
          if(res.result()!=null){
            msg.reply(res.result());
          }else{
            msg.fail(404,"Error updating");
          }
        }else{
          msg.fail(400,"Error updating");
        }
      });
    });
    eb.consumer("delete.doc.id",msg -> {
      String id=msg.body().toString();
      JsonObject query=new JsonObject().put("_id",id);
      client.findOneAndDelete("files",query,res->{
        if(res.succeeded()){
          if(res.result()!=null){
            msg.reply(res.result());
          }else{
            msg.fail(404,"Error deleting");
          }
        }else{
          msg.fail(400,"Error deleting");
        }
      });
    });
  }
}
