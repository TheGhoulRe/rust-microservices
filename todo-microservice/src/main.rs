
/*

NOTE
* JSON format: { "action": "string", "value": "anything" }

*/


use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record};
use std::str;
use serde_json::{json, Value};

fn main() {
  // Create a list to hold the items of the todo list
  let mut todo_list: Vec<String> = vec![];

  // Create the consumer
  let mut consumer =
    Consumer::from_hosts(vec!["localhost:9092".to_owned()])
      .with_topic("todo-actions".to_owned())
      .with_fallback_offset(FetchOffset::Latest)
      .create()
      .unwrap();
  
  // Create the producer
  let mut producer =
    Producer::from_hosts(vec!["localhost:9092".to_owned()])
      .create()
      .unwrap();
  
  println!("Started...");
  
  // Listen to messages from the consumer
  loop {
    for ms in consumer.poll().unwrap().iter() {
      for m in ms.messages() {

        // Parse message from the kafka cluster
        let message_json = str::from_utf8(m.value).unwrap().to_owned();
        let message: Value = serde_json::from_str(&message_json).unwrap();

        if message["action"] == "add" {
          // Push message to todo list
          todo_list.push( message["value"].to_string() );
          
          // Send todo list as json to the kafka cluster
          let json_bytes = json!(&todo_list).to_string();
          let record = Record::from_value("todo-list", json_bytes.as_bytes() );
          producer.send(&record).unwrap();

        } else if message["action"] == "remove" {
          // Remove item from todo list
          todo_list.remove( message["value"].to_string().parse::<usize>().unwrap() );

          // Send todo list as json to the kafka cluster
          let json_bytes = json!(&todo_list).to_string();
          let record = Record::from_value("todo-list", json_bytes.as_bytes() );
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