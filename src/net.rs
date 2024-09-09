#[cfg(not(feature = "turmoil"))]
pub use tokio::net::*;

#[cfg(feature = "turmoil")]
pub use turmoil::net::*;
use turmoil::ToSocketAddrs;
#[cfg(feature = "turmoil")]
use crate::net::connector::TurmoilTcpStream;

#[cfg(feature = "turmoil")]
pub fn make_incoming<A: ToSocketAddrs>(addr: A) -> impl futures::Stream<Item=Result<TurmoilTcpStream, std::io::Error>> {
    async_stream::stream! {
                        let listener = TcpListener::bind(addr).await.unwrap();
                        loop {
                            yield listener.accept().await.map(|(s, _)| TurmoilTcpStream(s));
                        }
                    }
}
#[cfg(feature = "turmoil")]
pub mod connector {
    use hyper::Uri;
    use hyper_util::client::legacy::connect::Connected;
    use hyper_util::rt::TokioIo;
    use std::task::{Context, Poll};
    use std::{future::Future, pin::Pin};
    use tokio::io;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tonic::transport::server::TcpConnectInfo;
    use tower::Service;
    use turmoil::net::TcpStream;

    #[derive(Clone)]
    pub struct TurmoilTcpConnector;

    impl Service<Uri> for TurmoilTcpConnector {
        type Response = TokioIo<TurmoilTcpStream>;
        type Error = io::Error;
        type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, uri: Uri) -> Self::Future {
            Box::pin(async move {
                let stream = TcpStream::connect(uri.authority().unwrap().as_str()).await?;
                Ok(TokioIo::new(TurmoilTcpStream(stream)))
            })
        }
    }

    pub struct TurmoilTcpStream(pub TcpStream);

    impl hyper_util::client::legacy::connect::Connection for TurmoilTcpStream {
        fn connected(&self) -> Connected {
            Connected::new()
        }
    }

    impl tonic::transport::server::Connected for TurmoilTcpStream {
        type ConnectInfo = TcpConnectInfo;

        fn connect_info(&self) -> Self::ConnectInfo {
            TcpConnectInfo {
                local_addr: self.0.local_addr().ok(),
                remote_addr: self.0.peer_addr().ok(),
            }
        }
    }

    impl AsyncRead for TurmoilTcpStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for TurmoilTcpStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}