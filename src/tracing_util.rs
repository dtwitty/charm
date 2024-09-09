use tracing::Span;

pub trait SpanOption {
    fn in_scope<F: FnOnce() -> T, T>(&self, f: F) -> T;
}

impl SpanOption for Option<Span> {
    fn in_scope<F: FnOnce() -> T, T>(&self, f: F) -> T {
        match self {
            Some(span) => {
                let _enter = span.enter();
                f()
            }
            None => f(),
        }
    }
}
