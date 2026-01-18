use crate::error::Result;
use crate::protocol::coding::{Control, Data, OpCode};
use crate::protocol::frame::CloseFrame;
use crate::protocol::utf8::concat_bytes;
use crate::{
    error::WebSocketError,
    protocol::{coding::CloseCode, frame::Frame, utf8::Utf8Bytes},
};
use bytes::Bytes;
use futures_util::future::Either;
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

enum FragmentReaderState {
    Partial(Sender<Bytes>),
    Complete,
}

#[pin_project]
pub struct FragmentReader<T>
where
    T: Stream<Item = Result<Frame>>,
{
    /// for sending control messages to writer (e.g., Pong in response to Ping)
    control_tx: Sender<Message>,
    incomplete: Option<(Data, u64, VecDeque<Bytes>)>,
    state: FragmentReaderState,
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
            state: FragmentReaderState::Complete,
            inner,
        }
    }
}

pub struct DataReader {
    init: VecDeque<Bytes>,
    rx: Receiver<Bytes>,
}

impl DataReader {
    pub fn new(rx: Receiver<Bytes>, init: VecDeque<Bytes>) -> Self {
        Self { init, rx }
    }
}

impl Stream for DataReader {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.init.is_empty() {
            // Safety: we checked that init is not empty
            let bytes = self.init.pop_front().unwrap();
            return Poll::Ready(Some(Ok(bytes)));
        }

        match ready!(self.rx.poll_recv(cx)) {
            Some(bytes) => Poll::Ready(Some(Ok(bytes))),
            None => Poll::Ready(None),
        }
    }
}

type DecodeResult<T> = Poll<Option<Result<T>>>;

fn resolve_ctrl_frame(
    ctrl_code: Control,
    frame: Frame,
    ctrl_tx: &Sender<Message>,
) -> DecodeResult<Either<Message, (Data, DataReader)>> {
    let msg = match ctrl_code {
        Control::Ping => {
            let bytes = frame.into_payload();
            let res = ctrl_tx.try_send(Message::Pong(bytes.clone()));
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
            let res = ctrl_tx.try_send(Message::Close(code, reason.clone()));
            match res {
                Ok(_) => (),
                Err(e) => {
                    error!("Failed to send Close control message: {}", e);
                }
            }
            Message::Close(code, reason)
        }
        Control::Reserved(d) => {
            return Poll::Ready(Some(Err(WebSocketError::ReservedControlOpcode(d))));
        }
    };
    Poll::Ready(Some(Ok(Either::Left(msg))))
}

impl<T> Stream for FragmentReader<T>
where
    T: Stream<Item = Result<Frame>>,
{
    type Item = Result<Either<Message, (Data, DataReader)>>;

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

                    match &mut this.state {
                        // 返回 DataReader 以供大数据读取
                        FragmentReaderState::Partial(tx) => match opcode {
                            OpCode::Control(ctrl) => {
                                return resolve_ctrl_frame(ctrl, frame, this.control_tx);
                            }
                            OpCode::Data(data) => {
                                match data {
                                    Data::Continue => {
                                        // 发送数据到 DataReader
                                        let payload = frame.into_payload();
                                        let res = tx.try_send(payload);
                                        match res {
                                            Ok(_) => (),
                                            Err(e) => {
                                                error!("Failed to send data to DataReader: {}", e);
                                            }
                                        }
                                        if fin {
                                            *this.state = FragmentReaderState::Complete;
                                        }
                                    }
                                    Data::Text | Data::Binary | Data::Reserved(_) => {
                                        return Poll::Ready(Some(Err(
                                            WebSocketError::InvalidFragment,
                                        )));
                                    }
                                }
                            }
                        },
                        // 正常状态
                        FragmentReaderState::Complete => {
                            match opcode {
                                // 处理控制帧：它们可以穿插在分片中
                                OpCode::Control(ctrl) => {
                                    return resolve_ctrl_frame(ctrl, frame, this.control_tx);
                                }

                                // 处理数据帧
                                OpCode::Data(data_type) => {
                                    match data_type {
                                        Data::Continue => {
                                            let Some((_, size, buf)) = this.incomplete.as_mut()
                                            else {
                                                return Poll::Ready(Some(Err(
                                                    WebSocketError::InvalidContinuationFrame,
                                                )));
                                            };
                                            // TODO: 在这里检查 buf 的总长度，防止内存溢出
                                            *size += frame.payload().len() as u64;
                                            buf.push_back(frame.into_payload());

                                            if fin {
                                                let (op, _, parts) =
                                                    this.incomplete.take().unwrap();
                                                return Poll::Ready(Some(Ok(Either::Left(
                                                    assemble_message(op, parts)?,
                                                ))));
                                            }
                                            if *size > 10 * 1024 * 1024 {
                                                let (incomplete_data_type, _, parts) =
                                                    this.incomplete.take().unwrap();
                                                let (tx, rx) = tokio::sync::mpsc::channel(10);
                                                *this.state = FragmentReaderState::Partial(tx);
                                                return Poll::Ready(Some(Ok(Either::Right((
                                                    incomplete_data_type,
                                                    DataReader::new(rx, parts),
                                                )))));
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
                                                return Poll::Ready(Some(Ok(Either::Left(
                                                    assemble_message(data_type, {
                                                        let mut parts = VecDeque::new();
                                                        parts.push_back(frame.into_payload());
                                                        parts
                                                    })?,
                                                ))));
                                            } else {
                                                let payload = frame.into_payload();
                                                *this.incomplete =
                                                    Some((data_type, payload.len() as u64, {
                                                        let mut parts = VecDeque::new();
                                                        parts.push_back(payload);
                                                        parts
                                                    }));
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
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            }
        }
    }
}

// 辅助函数：处理 UTF-8 校验和拼接
fn assemble_message(data_type: Data, parts: VecDeque<Bytes>) -> Result<Message> {
    match data_type {
        Data::Text => {
            let text = Utf8Bytes::try_from(parts)?;
            Ok(Message::Text(text))
        }
        Data::Binary => {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_small_message() {
        let msg = Message::Text("hello".into());
        let frames: Vec<Frame> = msg.into();
        assert_eq!(frames.len(), 1);
        let frame = &frames[0];
        assert_eq!(frame.header().opcode, OpCode::Data(Data::Text));
        assert_eq!(frame.payload(), Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_large_message() {
        // more than 512kb
        let large_data = vec![0u8; 600 * 1024];
        let msg = Message::Binary(Bytes::from(large_data.clone()));
        let frames: Vec<Frame> = msg.into();
        assert!(frames.len() > 1);
        let mut reassembled = Vec::new();
        for (i, frame) in frames.iter().enumerate() {
            if i == 0 {
                assert!(!frame.header().is_final);
                assert_eq!(frame.header().opcode, OpCode::Data(Data::Binary));
            } else if i == frames.len() - 1 {
                assert!(frame.header().is_final);
                assert_eq!(frame.header().opcode, OpCode::Data(Data::Continue));
            } else {
                assert!(!frame.header().is_final);
                assert_eq!(frame.header().opcode, OpCode::Data(Data::Continue));
            }

            reassembled.extend_from_slice(&frame.payload());
        }
        assert_eq!(reassembled, large_data);
    }
}
