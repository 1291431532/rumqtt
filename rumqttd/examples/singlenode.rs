use figment::{
    providers::{Format, Toml},
    Figment,
};
use rumqttd::{Broker, Config, Notification, Link};
use std::thread;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

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

    // link_tx.subscribe("hello/+/world").unwrap();
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

static CONFIG: &'static str = r#"
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
        [servers.1]
        listen = "0.0.0.0:1884"
        next_connection_delay_ms = 1
            [servers.1.connections]
            connection_timeout_ms = 20
            max_client_id_len = 256
            throttle_delay_ms = 0
            max_payload_size = 5120
            max_inflight_count = 200
            max_inflight_size = 1024

            [servers.1.tls_config]
            cert_path = "broker/examples/localhost.cert.pem"
            key_path = "broker/examples/localhost.key.pem"
            ca_path = "broker/examples/ca.cert.pem"

        # Configuration of server and connections that it accepts
        [shadows.1]
        listen = "0.0.0.0:5884"
        end_user_auth = "https://demo.bytebeam.io/api/v1/auth-end-user"
        next_connection_delay_ms = 1
            [shadows.1.connections]
            connection_timeout_ms = 10000 # TODO original 100
            max_client_id_len = 256
            throttle_delay_ms = 0
            max_payload_size = 20480
            max_inflight_count = 500
            max_inflight_size = 1024

        [console]
        listen = "0.0.0.0:3031"
    "#;
