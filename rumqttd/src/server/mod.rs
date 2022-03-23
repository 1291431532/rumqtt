use std::fs::File;

#[cfg(feature = "native-tls")]
use std::io::Read;
#[cfg(feature = "native-tls")]
use tokio_native_tls::native_tls;
#[cfg(feature = "native-tls")]
use tokio_native_tls::native_tls::Error as NativeTlsError;

use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "rustls")]
use tokio_rustls::rustls::internal::pemfile::{certs, rsa_private_keys};
#[cfg(feature = "rustls")]
use tokio_rustls::rustls::{
    AllowAnyAuthenticatedClient, RootCertStore, ServerConfig, TLSError as RustlsError,
};

#[cfg(feature = "rustls")]
use std::{io::BufReader, sync::Arc};

mod broker;

pub use self::broker::Broker;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[cfg(feature = "native-tls")]
    #[error("Native TLS error {0}")]
    NativeTls(#[from] NativeTlsError),
    #[cfg(feature = "rustls")]
    #[error("Rustls error {0}")]
    Rustls(#[from] RustlsError),
    #[error("Server cert file {0} not found")]
    ServerCertNotFound(String),
    #[error("Invalid server cert file {0}")]
    InvalidServerCert(String),
    #[error("Invalid CA cert file {0}")]
    InvalidCACert(String),
    #[error("Invalid server key file {0}")]
    InvalidServerKey(String),
    #[error("Server private key file {0} not found")]
    ServerKeyNotFound(String),
    #[error("CA file {0} no found")]
    CaFileNotFound(String),
    #[cfg(not(feature = "native-tls"))]
    NativeTlsNotEnabled,
    #[cfg(not(feature = "rustls"))]
    RustlsNotEnabled,
}

#[allow(dead_code)]
#[non_exhaustive]
enum TLSAcceptor {
    #[cfg(feature = "rustls")]
    Rustls { acceptor: tokio_rustls::TlsAcceptor },
    #[cfg(feature = "native-tls")]
    NativeTLS {
        acceptor: tokio_native_tls::TlsAcceptor,
    },
}

#[cfg(feature = "native-tls")]
fn create_nativetls_acceptor(pkcs12_path: &str, pkcs12_pass: &str) -> Result<TLSAcceptor, Error> {
    // Get certificates
    let cert_file = File::open(&pkcs12_path);
    let mut cert_file = cert_file.map_err(|_| Error::ServerCertNotFound(pkcs12_path.to_owned()))?;

    // Read cert into memory
    let mut buf = Vec::new();
    cert_file
        .read_to_end(&mut buf)
        .map_err(|_| Error::InvalidServerCert(pkcs12_path.to_owned()))?;

    // Get the identity
    let identity = native_tls::Identity::from_pkcs12(&buf, pkcs12_pass)
        .map_err(|_| Error::InvalidServerCert(pkcs12_path.to_owned()))?;

    // Builder
    let builder = native_tls::TlsAcceptor::builder(identity).build()?;

    // Create acceptor
    let acceptor = tokio_native_tls::TlsAcceptor::from(builder);
    Ok(TLSAcceptor::NativeTLS { acceptor })
}

#[allow(dead_code)]
#[cfg(not(feature = "native-tls"))]
fn create_nativetls_acceptor(_pkcs12_path: &str, _pkcs12_pass: &str) -> Result<TLSAcceptor, Error> {
    Err(Error::NativeTlsNotEnabled)
}

#[cfg(feature = "rustls")]
fn create_rustls_acceptor(
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
) -> Result<TLSAcceptor, Error> {
    let (certs, key) = {
        // Get certificates
        let cert_file = File::open(&cert_path);
        let cert_file = cert_file.map_err(|_| Error::ServerCertNotFound(cert_path.to_owned()))?;
        let certs = certs(&mut BufReader::new(cert_file));
        let certs = certs.map_err(|_| Error::InvalidServerCert(cert_path.to_string()))?;

        // Get private key
        let key_file = File::open(&key_path);
        let key_file = key_file.map_err(|_| Error::ServerKeyNotFound(key_path.to_owned()))?;
        let keys = rsa_private_keys(&mut BufReader::new(key_file));
        let keys = keys.map_err(|_| Error::InvalidServerKey(key_path.to_owned()))?;

        // Get the first key
        let key = match keys.first() {
            Some(k) => k.clone(),
            None => return Err(Error::InvalidServerKey(key_path.to_owned())),
        };

        (certs, key)
    };

    // client authentication with a CA. CA isn't required otherwise
    let mut server_config = {
        let ca_file = File::open(ca_path);
        let ca_file = ca_file.map_err(|_| Error::CaFileNotFound(ca_path.to_owned()))?;
        let ca_file = &mut BufReader::new(ca_file);
        let mut store = RootCertStore::empty();
        let o = store.add_pem_file(ca_file);
        o.map_err(|_| Error::InvalidCACert(ca_path.to_string()))?;
        // ServerConfig::new(NoClientAuth::new())
        ServerConfig::new(AllowAnyAuthenticatedClient::new(store))
    };

    server_config.set_single_cert(certs, key)?;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    Ok(TLSAcceptor::Rustls { acceptor })
}

#[allow(dead_code)]
#[cfg(not(feature = "rustls"))]
fn create_rustls_acceptor(
    _cert_path: &str,
    _key_path: &str,
    _ca_path: &str,
) -> Result<TLSAcceptor, Error> {
    Err(Error::RustlsNotEnabled)
}

pub trait IO: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> IO for T {}
