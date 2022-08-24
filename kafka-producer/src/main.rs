use kafka::producer::{Producer, Record};

fn main() {
  // create the producer
  let mut producer =
    Producer::from_hosts(vec!("localhost:9092".to_owned()))
      .create()
      .unwrap();
  
  // send a numbers 0 through 9 to the topic named "topic-name"
  for i in 0..10 {
    let buf = format!("{i}");
    producer.send(&Record::from_value("my-topic", buf.as_bytes())).unwrap();
    println!("Sent: {i}");
  }
}