//! Example of how to configure rumqttd to connect to a server using TLS and authentication.
use std::error::Error;

#[cfg(feature = "use-rustls")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    use rumqttc::{self, AsyncClient, Key, MqttOptions, TlsConfiguration, Transport};

    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqtt_options = MqttOptions::new("test-1", "localhost", 8883);
    mqtt_options.set_keep_alive(std::time::Duration::from_secs(5));

    // Dummies to prevent compilation error in CI
    let ca = vec![1, 2, 3];
    let client_cert = vec![1, 2, 3];
    let client_key = vec![1, 2, 3];
    //     let ca = include_bytes!("/home/tekjar/tlsfiles/ca.cert.pem");
    //     let client_cert = include_bytes!("/home/tekjar/tlsfiles/device-1.cert.pem");
    //     let client_key = include_bytes!("/home/tekjar/tlsfiles/device-1.key.pem");

    let transport = Transport::Tls(TlsConfiguration::Simple {
        ca: ca.to_vec(),
        alpn: None,
        client_auth: Some((client_cert.to_vec(), Key::RSA(client_key.to_vec()))),
    });

    mqtt_options.set_transport(transport);

    let (_client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    loop {
        match eventloop.poll().await {
            Ok(v) => {
                println!("Event = {:?}", v);
            }
            Err(e) => {
                println!("Error = {:?}", e);
                break;
            }
        }
    }

    Ok(())
}

#[cfg(not(feature = "use-rustls"))]
fn main() -> Result<(), Box<dyn Error>> {
    panic!("Enable feature 'use-rustls'");
}
