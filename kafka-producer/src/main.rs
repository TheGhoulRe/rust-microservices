use std::time::Duration;
use kafka::producer::{Producer, Record, RequiredAcks};

fn main() {
  let mut producer =
    Producer::from_hosts(vec!("localhost:9092".to_owned()))
      .with_ack_timeout(Duration::from_secs(1))
      .with_required_acks(RequiredAcks::One)
      .create()
      .unwrap();
  
  for i in 0..10 {
    let buf = format!("[{i}]");
    producer.send(&Record::from_value("my-topic", buf.as_bytes())).unwrap();
    println!("Sent: {i}");
  }
}