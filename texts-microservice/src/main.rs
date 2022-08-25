use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record};
use std::str;
use serde_json::{json, Value};

/*
* JSON format: { "action": "add|remove", "value": "string|number" }
*/

fn main() {
 // create a list to hold the texts
 let mut texts: Vec<String> = vec![];

 // create the consumer
 let mut consumer =
   Consumer::from_hosts(vec!["localhost:9092".to_owned()])
     .with_topic("actions".to_owned())
     .with_fallback_offset(FetchOffset::Latest)
     .create()
     .unwrap();

  // create the producer
  let mut producer =
    Producer::from_hosts(vec!["localhost:9092".to_owned()])
      .create()
      .unwrap();

    println!("Started...");

  // Listen to events from the actions topic consumer
  loop {
    for ms in consumer.poll().unwrap().iter() {
      for m in ms.messages() { // this block is executed if it picks up an event

        // parse event from the consumer
        let event_json = str::from_utf8(m.value).unwrap().to_owned();
        let event: Value = serde_json::from_str(&event_json).unwrap();

        if event["action"] == "add" {

          // add “value” from event to "texts"
          texts.push( event["value"].to_string() );
          
          // publish list to the "texts" topic
          let json_bytes = json!(&texts).to_string();
          let record = Record::from_value("texts", json_bytes.as_bytes() );
          producer.send(&record).unwrap();

        } else if event["action"] == "remove" {

          // use "index" to remove item from "texts"
          texts.remove( event["value"].to_string().parse::<usize>().unwrap() );

          // publish list to the "texts" topic
          let json_bytes = json!(&texts).to_string();
          let record = Record::from_value("texts", json_bytes.as_bytes() );
          producer.send(&record).unwrap();

        } else {
          println!("Invalid action");
        }

      }

      consumer.consume_messageset(ms).unwrap();
    }

    consumer.commit_consumed().unwrap();
  }
}
