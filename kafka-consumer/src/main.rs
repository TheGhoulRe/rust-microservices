use kafka::consumer::{Consumer, FetchOffset};
use std::str;

fn main () {
  // Create a consumer for the "topic-name" topic
  let mut consumer =
    Consumer::from_hosts(vec!("localhost:9092".to_owned()))
      .with_topic_partitions("topic-name".to_owned(), &[0])
      .with_fallback_offset(FetchOffset::Latest)
      .create()
      .unwrap();
  
  // Create a listener to listen to events
  loop {
    for ms in consumer.poll().unwrap().iter() {
      for m in ms.messages() {
        // convert message in variable m from bytes to string
        println!("{:?}", str::from_utf8(m.value).unwrap());
      }

      consumer.consume_messageset(ms).unwrap();
    }

    consumer.commit_consumed().unwrap();
  }
}