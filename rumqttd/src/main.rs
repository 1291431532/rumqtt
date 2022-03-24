use figment::{
    providers::{Format, Toml},
    Figment,
};
use rumqttd::{Broker, Config, Notification, Link};
use std::thread;

fn main() {
    pretty_env_logger::init();

    let config: Config = Figment::new()
        .merge(Toml::string(CONFIG))
        .extract()
        .unwrap();

    let mut broker = Broker::new(config);
    let Link(_link_tx, mut link_rx) = broker.link("singlenode").unwrap();
    thread::spawn(move || {
        broker.start().unwrap();
    });

    let mut count = 0;
    loop {
        let notification = match link_rx.recv().unwrap() {
            Some(v) => v,
            None => continue,
        };

        match notification {
            Notification::Forward { topic, payload, .. } => {
                count += 1;
                println!(
                    "Topic = {:?}, Count = {}, Payload = {} bytes",
                    topic,
                    count,
                    payload.len()
                );
            }
            v => {
                println!("{:?}", v);
            }
        }
    }
}

static CONFIG: &str = r#"
        # Broker id. Used to identify local node of the replication mesh
        id = 0

        # A commitlog read will pull full segment. Make sure that a segment isn't
        # too big as async tcp writes readiness of one connection might affect tail
        # latencies of other connection. Not a problem with preempting runtimes
        [router]
        id = 0
        instant_ack = true
        max_segment_size = 1048576
        max_mem_segments = 10
        max_disk_segments = 0
        max_read_len = 10240
        max_connections = 10001

        # Configuration of server and connections that it accepts
        [tcp.1]
        listen = "0.0.0.0:1883"
        next_connection_delay_ms = 1
            [tcp.1.connections]
            connection_timeout_ms = 30
            max_client_id_len = 256
            throttle_delay_ms = 0
            max_payload_size = 5120
            max_inflight_count = 200
            max_inflight_size = 1024

        # Configuration of server and connections that it accepts
        [websocket.1]
        listen = "0.0.0.0:5883"
        next_connection_delay_ms = 1
            [websocket.1.connections]
            connection_timeout_ms = 10000 # TODO original 100
            max_client_id_len = 256
            throttle_delay_ms = 0
            max_payload_size = 20480
            max_inflight_count = 500
            max_inflight_size = 1024

        [console]
        listen = "0.0.0.0:3030"

        [bridge]
        url = "localhost"
        port = 1884
        qos = 1
        reconnection_delay = 10
        ping_delay = 10
        timeout_delay = 10
        sub_path = "hello/+/world"
            [bridge.transport.tls]
            ca = "broker/examples/ca.cert.pem"
                [bridge.transport.tls.client_auth]
                certs = 'broker/examples/bridge.cert.pem'
                key = 'broker/examples/bridge.key.pem'
    "#;
