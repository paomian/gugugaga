use crate::error::Result;
use crate::frame::coding::{Control, Data, OpCode};
use crate::frame::frame::CloseFrame;
use crate::{
    error::WebSocketError,
    frame::{coding::CloseCode, frame::Frame, utf8::Utf8Bytes},
};
use bytes::Bytes;
use futures_util::{Sink, Stream, ready};
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum Message {
    Text(Utf8Bytes),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close(CloseCode, Option<Utf8Bytes>),
    Frame(Frame),
}

impl From<Message> for Vec<Frame> {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Frame(frame) => vec![frame],
            Message::Text(text) => {
                Frame::split_frame(text.into(), OpCode::Data(Data::Text), 1024 * 512)
            }
            Message::Binary(bin) => Frame::split_frame(bin, OpCode::Data(Data::Binary), 1024 * 512),
            Message::Ping(payload) => vec![Frame::ping(payload)],
            Message::Pong(payload) => vec![Frame::pong(payload)],
            Message::Close(code, reason) => {
                vec![Frame::close(reason.map(|r| CloseFrame { code, reason: r }))]
            }
        }
    }
}

#[pin_project]
pub struct FragmentReader<T>
where
    T: Stream<Item = Result<Frame>> + Unpin,
{
    incomplete: Option<(OpCode, Vec<Bytes>)>,
    #[pin]
    inner: T,
}

impl<T> FragmentReader<T>
where
    T: Stream<Item = Result<Frame>> + Unpin,
{
    pub fn new(inner: T) -> Self {
        Self {
            incomplete: None,
            inner,
        }
    }
}

impl<T> Stream for FragmentReader<T>
where
    T: Stream<Item = Result<Frame>> + Unpin,
{
    type Item = Result<Message>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let mut inner = this.inner;
        let incomplete = this.incomplete;
        loop {
            match inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    let opcode = frame.header().opcode;
                    let fin = frame.header().is_final;

                    match opcode {
                        OpCode::Data(Data::Text) => {
                            if incomplete.is_some() {
                                return Poll::Ready(Some(Err(WebSocketError::InvalidFragment)));
                            }
                            if fin {
                                let text = Utf8Bytes::try_from(frame.into_payload())?;
                                return Poll::Ready(Some(Ok(Message::Text(text))));
                            } else {
                                *incomplete = Some((opcode, vec![frame.into_payload()]));
                                continue;
                            }
                        }
                        OpCode::Data(Data::Binary) => {
                            if incomplete.is_some() {
                                return Poll::Ready(Some(Err(WebSocketError::InvalidFragment)));
                            }
                            if fin {
                                return Poll::Ready(Some(Ok(Message::Binary(
                                    frame.into_payload(),
                                ))));
                            } else {
                                *incomplete = Some((opcode, vec![frame.into_payload()]));
                                continue;
                            }
                        }
                        OpCode::Data(Data::Continue) => {
                            let Some((base_opcode, mut buf)) = incomplete.take() else {
                                return Poll::Ready(Some(Err(
                                    WebSocketError::InvalidContinuationFrame,
                                )));
                            };
                            buf.push(frame.into_payload());

                            if fin {
                                match base_opcode {
                                    OpCode::Data(Data::Text) => {
                                        let text = Utf8Bytes::try_from(buf)?;
                                        return Poll::Ready(Some(Ok(Message::Text(text))));
                                    }
                                    OpCode::Data(Data::Binary) => {
                                        return Poll::Ready(Some(Ok(Message::Binary(
                                            buf.into_iter().flatten().collect(),
                                        ))));
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                *incomplete = Some((base_opcode, buf));
                                continue;
                            }
                        }
                        OpCode::Data(Data::Reserved(d)) => {
                            return Poll::Ready(Some(Err(WebSocketError::ReservedDataOpcode(d))));
                        }
                        OpCode::Control(Control::Ping) => {
                            return Poll::Ready(Some(Ok(Message::Ping(frame.into_payload()))));
                        }
                        OpCode::Control(Control::Pong) => {
                            return Poll::Ready(Some(Ok(Message::Pong(frame.into_payload()))));
                        }
                        OpCode::Control(Control::Close) => {
                            let close = frame.into_close()?;
                            let (code, reason) = match close {
                                Some(c) => (c.code, Some(c.reason)),
                                None => (CloseCode::Status, None),
                            };
                            return Poll::Ready(Some(Ok(Message::Close(code, reason))));
                        }
                        OpCode::Control(Control::Reserved(d)) => {
                            return Poll::Ready(Some(Err(WebSocketError::ReservedControlOpcode(
                                d,
                            ))));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[pin_project]
struct FragmentWriter<T>
where
    T: Sink<Frame, Error = WebSocketError>,
{
    #[pin]
    inner: T,
    pending_frames: VecDeque<Frame>,
}

impl<T> FragmentWriter<T>
where
    T: Sink<Frame, Error = WebSocketError>,
{
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            pending_frames: VecDeque::new(),
        }
    }

    fn poll_flush_pending(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();

        while let Some(frame) = this.pending_frames.pop_front() {
            // 必须先检查底层是否准备好接收一个 frame
            ready!(this.inner.as_mut().poll_ready(cx))?;

            // 喂给底层
            if let Err(e) = this.inner.as_mut().start_send(frame) {
                return Poll::Ready(Err(e));
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl<T> Sink<Message> for FragmentWriter<T>
where
    T: Sink<Frame, Error = WebSocketError>,
{
    type Error = WebSocketError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.as_mut().poll_flush_pending(cx))?;
        self.project().inner.as_mut().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<()> {
        let pending_frames = self.project().pending_frames;
        let frames: Vec<Frame> = item.into();
        pending_frames.extend(frames);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.as_mut().poll_flush_pending(cx))?;
        // 2. 真正 flush 底层
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        // 1. 清空队列
        ready!(self.as_mut().poll_flush_pending(cx))?;
        // 2. 逐级关闭
        self.project().inner.poll_close(cx)
    }
}
