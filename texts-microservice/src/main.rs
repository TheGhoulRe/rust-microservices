use kafka::consumer::{Consumer, FetchOffset, Message};
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
      for m in ms.messages() {
        consume_event(m, &mut texts, &mut producer);
      }

      consumer.consume_messageset(ms).unwrap();
    }

    consumer.commit_consumed().unwrap();
  }
}

fn consume_event( m: &Message, texts: &mut Vec<String>, producer: &mut Producer ) {

  // convert event message from bytes to string
  let event_json = str::from_utf8(m.value).unwrap().to_owned();

  // parse event from the consumer
  let event: Value = serde_json::from_str(&event_json).unwrap();

  // get action to perform from event
  let action: String = event["action"].to_string();

  if action == "add" {
    add_text(&event, texts, producer);
  } else if action == "remove" {
    remove_text(&event, texts, producer);
  } else {
    println!("Invalid action");
  }
}

fn add_text(event: &Value, texts: &mut Vec<String>, producer: &mut Producer) {

  // get text from event JSON
  let text = event["value"].to_string();

  // add text to list
  texts.push(text);
    
  // convert list to JSON
  let json_bytes = json!(&texts).to_string();

  // prepare for publishing
  let record = Record::from_value("texts", json_bytes.as_bytes() );

  // publish to broker
  producer.send(&record).unwrap();
}

fn remove_text(event: &Value, texts: &mut Vec<String>, producer: &mut Producer) {

  // get index of text to remove
  let index = event["value"].to_string().parse::<usize>().unwrap();

  // remove text on specified index
  texts.remove( index );

  // convert list to JSON
  let json_bytes = json!(&texts).to_string();

  // prepare for publishing
  let record = Record::from_value("texts", json_bytes.as_bytes() );

  // publish to broker
  producer.send(&record).unwrap();
}