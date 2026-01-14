use crate::error::Result;
use crate::protocol::coding::{Control, Data, OpCode};
use crate::protocol::frame::CloseFrame;
use crate::protocol::utf8::concat_bytes;
use crate::{
    error::WebSocketError,
    protocol::{coding::CloseCode, frame::Frame, utf8::Utf8Bytes},
};
use bytes::Bytes;
use futures_util::{Sink, Stream, ready};
use log::{error, warn};
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
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
    T: Stream<Item = Result<Frame>>,
{
    control_tx: Sender<Message>,
    incomplete: Option<(OpCode, Vec<Bytes>)>,
    #[pin]
    inner: T,
}

impl<T> FragmentReader<T>
where
    T: Stream<Item = Result<Frame>>,
{
    pub fn new(inner: T, control_tx: Sender<Message>) -> Self {
        Self {
            control_tx,
            incomplete: None,
            inner,
        }
    }
}

impl<T> Stream for FragmentReader<T>
where
    T: Stream<Item = Result<Frame>>,
{
    type Item = Result<Message>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match ready!(this.inner.as_mut().poll_next(cx)) {
                Some(Ok(frame)) => {
                    let opcode = frame.header().opcode;
                    let fin = frame.header().is_final;
                    if frame.is_masked() {
                        return Poll::Ready(Some(Err(WebSocketError::MaskedFrameFromServer)));
                    }

                    match opcode {
                        // 处理控制帧：它们可以穿插在分片中
                        OpCode::Control(ctrl) => {
                            let msg = match ctrl {
                                Control::Ping => {
                                    let bytes = frame.into_payload();
                                    let res =
                                        this.control_tx.try_send(Message::Pong(bytes.clone()));
                                    match res {
                                        Ok(_) => (),
                                        Err(e) => {
                                            error!("Failed to send Pong control message: {}", e);
                                        }
                                    }
                                    Message::Ping(bytes)
                                }
                                Control::Pong => Message::Pong(frame.into_payload()),
                                Control::Close => {
                                    let close = frame.into_close()?;
                                    let (code, reason) = close
                                        .map(|c| (c.code, Some(c.reason)))
                                        .unwrap_or((CloseCode::Status, None));
                                    let res = this
                                        .control_tx
                                        .try_send(Message::Close(code, reason.clone()));
                                    match res {
                                        Ok(_) => (),
                                        Err(e) => {
                                            error!("Failed to send Close control message: {}", e);
                                        }
                                    }
                                    Message::Close(code, reason)
                                }
                                Control::Reserved(d) => {
                                    return Poll::Ready(Some(Err(
                                        WebSocketError::ReservedControlOpcode(d),
                                    )));
                                }
                            };
                            return Poll::Ready(Some(Ok(msg)));
                        }

                        // 处理数据帧
                        OpCode::Data(data_type) => {
                            match data_type {
                                Data::Continue => {
                                    let Some((_, buf)) = this.incomplete.as_mut() else {
                                        return Poll::Ready(Some(Err(
                                            WebSocketError::InvalidContinuationFrame,
                                        )));
                                    };

                                    // TODO: 在这里检查 buf 的总长度，防止内存溢出
                                    buf.push(frame.into_payload());

                                    if fin {
                                        let (op, parts) = this.incomplete.take().unwrap();
                                        return Poll::Ready(Some(Ok(assemble_message(op, parts)?)));
                                    }
                                    // 没完，继续 loop 读下一帧
                                }
                                Data::Text | Data::Binary => {
                                    if this.incomplete.is_some() {
                                        return Poll::Ready(Some(Err(
                                            WebSocketError::InvalidFragment,
                                        )));
                                    }
                                    if fin {
                                        return Poll::Ready(Some(Ok(assemble_message(
                                            opcode,
                                            vec![frame.into_payload()],
                                        )?)));
                                    } else {
                                        *this.incomplete =
                                            Some((opcode, vec![frame.into_payload()]));
                                    }
                                }
                                Data::Reserved(d) => {
                                    return Poll::Ready(Some(Err(
                                        WebSocketError::ReservedDataOpcode(d),
                                    )));
                                }
                            }
                        }
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            }
        }
    }
}

// 辅助函数：处理 UTF-8 校验和拼接
fn assemble_message(opcode: OpCode, parts: Vec<Bytes>) -> Result<Message> {
    match opcode {
        OpCode::Data(Data::Text) => {
            let text = Utf8Bytes::try_from(parts)?;
            Ok(Message::Text(text))
        }
        OpCode::Data(Data::Binary) => {
            let bytes = concat_bytes(parts);
            Ok(Message::Binary(bytes))
        }
        _ => unreachable!(),
    }
}

#[pin_project]
pub struct FragmentWriter<T>
where
    T: Sink<Frame, Error = WebSocketError>,
{
    #[pin]
    inner: T,
    pending_frames: VecDeque<Frame>,
    control_rx: Receiver<Message>,
}

impl<T> FragmentWriter<T>
where
    T: Sink<Frame, Error = WebSocketError>,
{
    pub fn new(inner: T, control_rx: Receiver<Message>) -> Self {
        Self {
            inner,
            pending_frames: VecDeque::new(),
            control_rx,
        }
    }

    fn poll_flush_pending(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();

        while let Poll::Ready(Some(msg)) = this.control_rx.poll_recv(cx) {
            match msg {
                Message::Pong(payload) => this.pending_frames.push_back(Frame::pong(payload)),
                Message::Close(code, reason) => {
                    this.pending_frames
                        .push_back(Frame::close(reason.map(|r| CloseFrame { code, reason: r })));
                }
                _ => {
                    warn!("Unexpected message in control channel: {:?}", msg);
                }
            }
        }

        while let Some(frame) = this.pending_frames.pop_front() {
            ready!(this.inner.as_mut().poll_ready(cx))?;
            this.inner.as_mut().start_send(frame)?;
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
        for mut frame in frames {
            frame.set_random_mask();
            pending_frames.push_back(frame);
        }
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.as_mut().poll_flush_pending(cx))?;
        // 2. 真正 flush 底层
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.as_mut().project().control_rx.close();

        // 1. 清空队列
        ready!(self.as_mut().poll_flush_pending(cx))?;
        // 2. 逐级关闭
        self.project().inner.poll_close(cx)
    }
}
