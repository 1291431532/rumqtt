use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::BufMut;
use smol::io;
use smol::prelude::*;

// use pin_project_lite::pin_project;

pub enum Return5<T1, T2, T3, T4, T5> { R1(T1), R2(T2), R3(T3), R4(T4), R5(T5) }

pub struct Select5<F1, F2, F3, F4, F5, FR1, FR2, FR3, FR4, FR5> {
    pub future1: Option<F1>,
    pub future2: Option<F2>,
    pub future3: Option<F3>,
    pub future4: Option<F4>,
    pub future5: Option<F5>,
    pub fr1: FR1,
    pub fr2: FR2,
    pub fr3: FR3,
    pub fr4: FR4,
    pub fr5: FR5,
    pub start: u8,
}

impl<FR1, FR2, FR3, FR4, FR5, F1: Future, F2: Future, F3: Future, F4: Future, F5: Future> Future for Select5<F1, F2, F3, F4, F5, FR1, FR2, FR3, FR4, FR5>
    where
        FR1: Fn(&F1::Output) -> bool,
        FR2: Fn(&F2::Output) -> bool,
        FR3: Fn(&F3::Output) -> bool,
        FR4: Fn(&F4::Output) -> bool,
        FR5: Fn(&F5::Output) -> bool,
{
    type Output = Return5<F1::Output, F2::Output, F3::Output, F4::Output, F5::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut current;
        let mut is_pending = false;
        for i in 0..5 {
            current = (this.start + i) % 5;
            unsafe {
                match current {
                    0 => {
                        if let Some(p1) = Pin::new_unchecked(&mut this.future1).as_pin_mut() {
                            match p1.poll(cx) {
                                Poll::Ready(r1) => {
                                    if (this.fr1)(&r1) {
                                        return Poll::Ready(Return5::R1(r1));
                                    }
                                    this.future1.take();
                                }
                                Poll::Pending => {}
                            }
                            is_pending = true;
                        }
                    }
                    1 => {
                        if let Some(p2) = Pin::new_unchecked(&mut this.future2).as_pin_mut() {
                            match p2.poll(cx) {
                                Poll::Ready(r2) => {
                                    if (this.fr2)(&r2) {
                                        return Poll::Ready(Return5::R2(r2));
                                    }
                                    this.future2.take();
                                }
                                Poll::Pending => {}
                            }
                            is_pending = true;
                        }
                    }
                    2 => {
                        if let Some(p3) = Pin::new_unchecked(&mut this.future3).as_pin_mut() {
                            match p3.poll(cx) {
                                Poll::Ready(r3) => {
                                    if (this.fr3)(&r3) {
                                        return Poll::Ready(Return5::R3(r3));
                                    }
                                    this.future3.take();
                                }
                                Poll::Pending => {}
                            }
                            is_pending = true;
                        }
                    }
                    3 => {
                        if let Some(p4) = Pin::new_unchecked(&mut this.future4).as_pin_mut() {
                            match p4.poll(cx) {
                                Poll::Ready(r4) => {
                                    if (this.fr4)(&r4) {
                                        return Poll::Ready(Return5::R4(r4));
                                    }
                                    this.future4.take();
                                }
                                Poll::Pending => {}
                            }
                            is_pending = true;
                        }
                    }
                    4 => {
                        if let Some(p5) = Pin::new_unchecked(&mut this.future5).as_pin_mut() {
                            match p5.poll(cx) {
                                Poll::Ready(r5) => {
                                    if (this.fr5)(&r5) {
                                        return Poll::Ready(Return5::R5(r5));
                                    }
                                    this.future5.take();
                                }
                                Poll::Pending => {}
                            }
                            is_pending = true;
                        }
                    }
                    _ => {}
                }
            }
        }
        if is_pending {
            return Poll::Pending;
        }
        panic!("Select5 NO FOUND DEFAULT!")
    }
}


pin_project_lite::pin_project! {
    /// Future returned by [`read_buf`](crate::io::AsyncReadExt::read_buf).
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ReadBuf<'a, R, B> {
        reader: &'a mut R,
        buf: &'a mut B,
        #[pin]
        _pin: PhantomPinned,
    }
}

pub fn read_buf<'a, R, B>(reader: &'a mut R, buf: &'a mut B) -> ReadBuf<'a, R, B>
    where
        R: AsyncRead + Unpin,
        B: BufMut,
{
    ReadBuf {
        reader,
        buf,
        _pin: PhantomPinned,
    }
}


impl<R, B> Future for ReadBuf<'_, R, B>
    where
        R: AsyncRead + Unpin,
        B: BufMut,
{
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        use std::mem::MaybeUninit;

        let me = self.project();

        if !me.buf.has_remaining_mut() {
            return Poll::Ready(Ok(0));
        }
        let n = {
            let buf = unsafe { &mut *(me.buf.chunk_mut() as *mut _ as *mut [MaybeUninit<u8>] as *mut [u8]) };
            // dst.as_mut_ptr().write_bytes(0, dst.len());
            smol::ready!(Pin::new(me.reader).poll_read(cx, buf)?)
        };
        unsafe {
            me.buf.advance_mut(n);
        }

        Poll::Ready(Ok(n))
    }
}