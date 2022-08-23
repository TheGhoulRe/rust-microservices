use kafka::consumer::{Consumer, FetchOffset};
use std::str::from_utf8;

fn main () {
  let mut consumer =
    Consumer::from_hosts(vec!("localhost:9092".to_owned()))
      .with_topic_partitions("my-topic".to_owned(), &[0])
      .with_fallback_offset(FetchOffset::Latest)
      .create()
      .unwrap();
  
  loop {
    for ms in consumer.poll().unwrap().iter() {
      for m in ms.messages() {
        println!("{:?}", from_utf8(m.value).unwrap());
      }

      consumer.consume_messageset(ms).unwrap();
    }

    consumer.commit_consumed().unwrap();
  }
}