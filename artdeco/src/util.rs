use futures::ready;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Sink;
use pin_project_lite::pin_project;

impl<T: ?Sized, Item> CustomSinkExt<Item> for T where T: Sink<Item> {}

pub(crate) trait CustomSinkExt<Item>: Sink<Item> {
    fn map<U, F>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(U) -> Item,
        Self: Sized,
    {
        Map { sink: self, f }
    }
}

pin_project! {
    #[must_use = "sinks do nothing unless polled"]
    pub(crate) struct Map<Si, F> {
        #[pin]
        sink: Si,
        f: F,
    }
}

impl<Si, F> fmt::Debug for Map<Si, F>
where
    Si: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Map").field("sink", &self.sink).finish()
    }
}

impl<Si, F> Clone for Map<Si, F>
where
    Si: Clone,
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            sink: self.sink.clone(),
            f: self.f.clone(),
        }
    }
}

impl<Si, Item, U, F> Sink<U> for Map<Si, F>
where
    Si: Sink<Item>,
    F: FnMut(U) -> Item,
{
    type Error = Si::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.project().sink.poll_ready(cx)?);
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: U) -> Result<(), Self::Error> {
        let this = self.project();
        this.sink.start_send((this.f)(item))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.project().sink.poll_flush(cx)?);
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.project().sink.poll_close(cx)?);
        Poll::Ready(Ok(()))
    }
}
