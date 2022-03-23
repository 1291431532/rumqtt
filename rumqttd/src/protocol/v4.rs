// we are allowing dead code as we may not use both protocol at once
#![allow(dead_code)]

use std::{slice::Iter, str::Utf8Error};

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) mod connect {
    use super::*;
    use bytes::Bytes;

    /// Connection packet initiated by the client
    #[derive(Debug, Clone, PartialEq)]
    pub struct Connect {
        /// Mqtt keep alive time
        pub keep_alive: u16,
        /// Client Id
        pub client_id: String,
        /// Clean session. Asks the broker to clear previous state
        pub clean_session: bool,
        /// Will that broker needs to publish when the client disconnects
        pub last_will: Option<LastWill>,
        /// Login credentials
        pub login: Option<Login>,
    }

    impl Connect {
        pub fn new<S: Into<String>>(id: S) -> Connect {
            Connect {
                keep_alive: 10,
                client_id: id.into(),
                clean_session: true,
                last_will: None,
                login: None,
            }
        }

        pub fn len(&self) -> usize {
            let mut len = 2 + "MQTT".len() // protocol name
                              + 1            // protocol version
                              + 1            // connect flags
                              + 2; // keep alive

            len += 2 + self.client_id.len();

            // last will len
            if let Some(last_will) = &self.last_will {
                len += last_will.len();
            }

            // username and password len
            if let Some(login) = &self.login {
                len += login.len();
            }

            len
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Connect, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            // Variable header
            let protocol_name = read_mqtt_bytes(&mut bytes)?;
            let protocol_name = std::str::from_utf8(&protocol_name)?.to_owned();
            let protocol_level = read_u8(&mut bytes)?;
            if protocol_name != "MQTT" {
                return Err(Error::InvalidProtocol);
            }

            if protocol_level != 4 {
                return Err(Error::InvalidProtocolLevel(protocol_level));
            }

            let connect_flags = read_u8(&mut bytes)?;
            let clean_session = (connect_flags & 0b10) != 0;
            let keep_alive = read_u16(&mut bytes)?;

            let client_id = read_mqtt_bytes(&mut bytes)?;
            let client_id = std::str::from_utf8(&client_id)?.to_owned();
            let last_will = LastWill::read(connect_flags, &mut bytes)?;
            let login = Login::read(connect_flags, &mut bytes)?;

            let connect = Connect {
                keep_alive,
                client_id,
                clean_session,
                last_will,
                login,
            };

            Ok(connect)
        }

        pub fn write(&self, buffer: &mut BytesMut) -> Result<usize, Error> {
            let len = self.len();
            buffer.put_u8(0b0001_0000);
            let count = write_remaining_length(buffer, len)?;
            write_mqtt_string(buffer, "MQTT");
            buffer.put_u8(0x04);

            let flags_index = 1 + count + 2 + 4 + 1;

            let mut connect_flags = 0;
            if self.clean_session {
                connect_flags |= 0x02;
            }

            buffer.put_u8(connect_flags);
            buffer.put_u16(self.keep_alive);
            write_mqtt_string(buffer, &self.client_id);

            if let Some(last_will) = &self.last_will {
                connect_flags |= last_will.write(buffer)?;
            }

            if let Some(login) = &self.login {
                connect_flags |= login.write(buffer);
            }

            // update connect flags
            buffer[flags_index] = connect_flags;
            Ok(len)
        }
    }

    /// LastWill that broker forwards on behalf of the client
    #[derive(Debug, Clone, PartialEq)]
    pub struct LastWill {
        pub topic: String,
        pub message: Bytes,
        pub qos: QoS,
        pub retain: bool,
    }

    impl LastWill {
        pub fn _new(
            topic: impl Into<String>,
            payload: impl Into<Vec<u8>>,
            qos: QoS,
            retain: bool,
        ) -> LastWill {
            LastWill {
                topic: topic.into(),
                message: Bytes::from(payload.into()),
                qos,
                retain,
            }
        }

        fn len(&self) -> usize {
            let mut len = 0;
            len += 2 + self.topic.len() + 2 + self.message.len();
            len
        }

        fn read(connect_flags: u8, bytes: &mut Bytes) -> Result<Option<LastWill>, Error> {
            let last_will = match connect_flags & 0b100 {
                0 if (connect_flags & 0b0011_1000) != 0 => {
                    return Err(Error::IncorrectPacketFormat);
                }
                0 => None,
                _ => {
                    let will_topic = read_mqtt_bytes(bytes)?;
                    let will_topic = std::str::from_utf8(&will_topic)?.to_owned();
                    let will_message = read_mqtt_bytes(bytes)?;
                    let will_qos = qos((connect_flags & 0b11000) >> 3)?;
                    Some(LastWill {
                        topic: will_topic,
                        message: will_message,
                        qos: will_qos,
                        retain: (connect_flags & 0b0010_0000) != 0,
                    })
                }
            };

            Ok(last_will)
        }

        fn write(&self, buffer: &mut BytesMut) -> Result<u8, Error> {
            let mut connect_flags = 0;

            connect_flags |= 0x04 | (self.qos as u8) << 3;
            if self.retain {
                connect_flags |= 0x20;
            }

            write_mqtt_string(buffer, &self.topic);
            write_mqtt_bytes(buffer, &self.message);
            Ok(connect_flags)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct Login {
        username: String,
        password: String,
    }

    impl Login {
        pub fn new<S: Into<String>>(u: S, p: S) -> Login {
            Login {
                username: u.into(),
                password: p.into(),
            }
        }

        fn read(connect_flags: u8, bytes: &mut Bytes) -> Result<Option<Login>, Error> {
            let username = match connect_flags & 0b1000_0000 {
                0 => String::new(),
                _ => {
                    let username = read_mqtt_bytes(bytes)?;
                    std::str::from_utf8(&username)?.to_owned()
                }
            };

            let password = match connect_flags & 0b0100_0000 {
                0 => String::new(),
                _ => {
                    let password = read_mqtt_bytes(bytes)?;
                    std::str::from_utf8(&password)?.to_owned()
                }
            };

            if username.is_empty() && password.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Login { username, password }))
            }
        }

        fn len(&self) -> usize {
            let mut len = 0;

            if !self.username.is_empty() {
                len += 2 + self.username.len();
            }

            if !self.password.is_empty() {
                len += 2 + self.password.len();
            }

            len
        }

        fn write(&self, buffer: &mut BytesMut) -> u8 {
            let mut connect_flags = 0;
            if !self.username.is_empty() {
                connect_flags |= 0x80;
                write_mqtt_string(buffer, &self.username);
            }

            if !self.password.is_empty() {
                connect_flags |= 0x40;
                write_mqtt_string(buffer, &self.password);
            }

            connect_flags
        }
    }
}

pub(crate) mod connack {
    use super::*;
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    /// Return code in connack
    #[derive(Debug, Clone, Copy, PartialEq)]
    #[repr(u8)]
    pub enum ConnectReturnCode {
        Success = 0,
        RefusedProtocolVersion,
        BadClientId,
        ServiceUnavailable,
        BadUserNamePassword,
        NotAuthorized,
    }

    /// Acknowledgement to connect packet
    #[derive(Debug, Clone, PartialEq)]
    pub struct ConnAck {
        pub session_present: bool,
        pub code: ConnectReturnCode,
    }

    impl ConnAck {
        pub fn new(code: ConnectReturnCode, session_present: bool) -> ConnAck {
            ConnAck {
                code,
                session_present,
            }
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            let flags = read_u8(&mut bytes)?;
            let return_code = read_u8(&mut bytes)?;

            let session_present = (flags & 0x01) == 1;
            let code = connect_return(return_code)?;
            let connack = ConnAck {
                session_present,
                code,
            };

            Ok(connack)
        }
    }

    pub fn write(
        code: ConnectReturnCode,
        session_present: bool,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        // sesssion present + code
        let len = 1 + 1;
        buffer.put_u8(0x20);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u8(session_present as u8);
        buffer.put_u8(code as u8);

        Ok(1 + count + len)
    }

    /// Connection return code type
    fn connect_return(num: u8) -> Result<ConnectReturnCode, Error> {
        match num {
            0 => Ok(ConnectReturnCode::Success),
            1 => Ok(ConnectReturnCode::RefusedProtocolVersion),
            2 => Ok(ConnectReturnCode::BadClientId),
            3 => Ok(ConnectReturnCode::ServiceUnavailable),
            4 => Ok(ConnectReturnCode::BadUserNamePassword),
            5 => Ok(ConnectReturnCode::NotAuthorized),
            num => Err(Error::InvalidConnectReturnCode(num)),
        }
    }
}

pub(crate) mod publish {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};

    #[derive(Debug, Clone, PartialEq)]
    pub struct Publish {
        pub fixed_header: FixedHeader,
        pub raw: Bytes,
    }

    impl Publish {
        //         pub fn new<S: Into<String>, P: Into<Vec<u8>>>(topic: S, qos: QoS, payload: P) -> Publish {
        //             Publish {
        //                 dup: false,
        //                 qos,
        //                 retain: false,
        //                 pkid: 0,
        //                 topic: topic.into(),
        //                 payload: Bytes::from(payload.into()),
        //             }
        //         }

        //         pub fn from_bytes<S: Into<String>>(topic: S, qos: QoS, payload: Bytes) -> Publish {
        //             Publish {
        //                 dup: false,
        //                 qos,
        //                 retain: false,
        //                 pkid: 0,
        //                 topic: topic.into(),
        //                 payload,
        //             }
        //         }

        //         pub fn len(&self) -> usize {
        //             let mut len = 2 + self.topic.len();
        //             if self.qos != QoS::AtMostOnce && self.pkid != 0 {
        //                 len += 2;
        //             }

        //             len += self.payload.len();
        //             len
        //         }

        pub fn view_meta(&self) -> Result<(&str, u8, u16, bool, bool), Error> {
            let qos = (self.fixed_header.byte1 & 0b0110) >> 1;
            let dup = (self.fixed_header.byte1 & 0b1000) != 0;
            let retain = (self.fixed_header.byte1 & 0b0001) != 0;

            // FIXME: Remove indexes and use get method
            let stream = &self.raw[self.fixed_header.fixed_header_len..];
            let topic_len = view_u16(stream)? as usize;

            let stream = &stream[2..];
            let topic = view_str(stream, topic_len)?;

            let pkid = match qos {
                0 => 0,
                1 => {
                    let stream = &stream[topic_len..];
                    view_u16(stream)?
                }
                v => return Err(Error::InvalidQoS(v)),
            };

            if qos == 1 && pkid == 0 {
                return Err(Error::PacketIdZero);
            }

            Ok((topic, qos, pkid, dup, retain))
        }

        pub fn view_topic(&self) -> Result<&str, Error> {
            // FIXME: Remove indexes
            let stream = &self.raw[self.fixed_header.fixed_header_len..];
            let topic_len = view_u16(stream)? as usize;

            let stream = &stream[2..];
            let topic = view_str(stream, topic_len)?;
            Ok(topic)
        }

        pub fn take_topic_and_payload(mut self) -> Result<(Bytes, Bytes), Error> {
            let qos = (self.fixed_header.byte1 & 0b0110) >> 1;

            let variable_header_index = self.fixed_header.fixed_header_len;
            self.raw.advance(variable_header_index);
            let topic = read_mqtt_bytes(&mut self.raw)?;

            match qos {
                0 => (),
                1 => self.raw.advance(2),
                v => return Err(Error::InvalidQoS(v)),
            };

            let payload = self.raw;
            Ok((topic, payload))
        }

        pub fn read(fixed_header: FixedHeader, bytes: Bytes) -> Result<Self, Error> {
            let publish = Publish {
                fixed_header,
                raw: bytes,
            };

            Ok(publish)
        }
    }

    pub struct PublishBytes(pub Bytes);

    impl From<PublishBytes> for Result<Publish, Error> {
        fn from(raw: PublishBytes) -> Self {
            let fixed_header = check(raw.0.iter(), 100 * 1024 * 1024)?;
            Ok(Publish {
                fixed_header,
                raw: raw.0,
            })
        }
    }

    pub fn write(
        topic: &str,
        qos: QoS,
        pkid: u16,
        dup: bool,
        retain: bool,
        payload: &[u8],
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        let mut len = 2 + topic.len();
        if qos != QoS::AtMostOnce {
            len += 2;
        }

        len += payload.len();

        let dup = dup as u8;
        let qos = qos as u8;
        let retain = retain as u8;

        buffer.put_u8(0b0011_0000 | retain | qos << 1 | dup << 3);

        let count = write_remaining_length(buffer, len)?;
        write_mqtt_string(buffer, topic);

        if qos != 0 {
            if pkid == 0 {
                return Err(Error::PacketIdZero);
            }

            buffer.put_u16(pkid);
        }

        buffer.extend_from_slice(payload);

        // TODO: Returned length is wrong in other packets. Fix it
        Ok(1 + count + len)
    }
}

pub(crate) mod puback {
    use super::*;
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    /// Acknowledgement to QoS1 publish
    #[derive(Debug, Clone, PartialEq)]
    pub struct PubAck {
        pub pkid: u16,
    }

    impl PubAck {
        pub fn new(pkid: u16) -> PubAck {
            PubAck { pkid }
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);
            let pkid = read_u16(&mut bytes)?;

            // No reason code or properties if remaining length == 2
            if fixed_header.remaining_len == 2 {
                return Ok(PubAck { pkid });
            }

            // No properties len or properties if remaining len > 2 but < 4
            if fixed_header.remaining_len < 4 {
                return Ok(PubAck { pkid });
            }

            let puback = PubAck { pkid };

            Ok(puback)
        }
    }

    pub fn write(pkid: u16, buffer: &mut BytesMut) -> Result<usize, Error> {
        let len = 2; // pkid
        buffer.put_u8(0x40);

        let count = write_remaining_length(buffer, len)?;
        buffer.put_u16(pkid);
        Ok(1 + count + len)
    }
}

pub(crate) mod subscribe {
    use super::*;
    use bytes::{Buf, Bytes};

    /// Subscription packet
    #[derive(Debug, Clone, PartialEq)]
    pub struct Subscribe {
        pub pkid: u16,
        pub filters: Vec<SubscribeFilter>,
    }

    impl Subscribe {
        pub fn new<S: Into<String>>(path: S, qos: QoS) -> Subscribe {
            let filter = SubscribeFilter {
                path: path.into(),
                qos,
            };

            let filters = vec![filter];
            Subscribe { pkid: 0, filters }
        }

        pub fn add(&mut self, path: String, qos: QoS) -> &mut Self {
            let filter = SubscribeFilter { path, qos };

            self.filters.push(filter);
            self
        }

        pub fn len(&self) -> usize {
            let len = 2 + self.filters.iter().fold(0, |s, t| s + t.len()); // len of pkid + vec![subscribe filter len]
            len
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            let pkid = read_u16(&mut bytes)?;

            // variable header size = 2 (packet identifier)
            let mut filters = Vec::new();

            while bytes.has_remaining() {
                let path = read_mqtt_bytes(&mut bytes)?;
                let path = std::str::from_utf8(&path)?.to_owned();
                let options = read_u8(&mut bytes)?;
                let requested_qos = options & 0b0000_0011;

                filters.push(SubscribeFilter {
                    path,
                    qos: qos(requested_qos)?,
                });
            }

            let subscribe = Subscribe { pkid, filters };

            Ok(subscribe)
        }
    }

    pub fn write(
        filters: Vec<SubscribeFilter>,
        pkid: u16,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        let len = 2 + filters.iter().fold(0, |s, t| s + t.len()); // len of pkid + vec![subscribe filter len]
                                                                  // write packet type
        buffer.put_u8(0x82);

        // write remaining length
        let remaining_len_bytes = write_remaining_length(buffer, len)?;

        // write packet id
        buffer.put_u16(pkid);

        // write filters
        for filter in filters.iter() {
            filter.write(buffer);
        }

        Ok(1 + remaining_len_bytes + len)
    }

    ///  Subscription filter
    #[derive(Debug, Clone, PartialEq)]
    pub struct SubscribeFilter {
        pub path: String,
        pub qos: QoS,
    }

    impl SubscribeFilter {
        pub fn new(path: String, qos: QoS) -> SubscribeFilter {
            SubscribeFilter { path, qos }
        }

        pub fn len(&self) -> usize {
            // filter len + filter + options
            2 + self.path.len() + 1
        }

        fn write(&self, buffer: &mut BytesMut) {
            let mut options = 0;
            options |= self.qos as u8;

            write_mqtt_string(buffer, self.path.as_str());
            buffer.put_u8(options);
        }
    }
}

pub(crate) mod suback {
    use std::convert::{TryFrom, TryInto};

    use super::*;
    use bytes::{Buf, Bytes};

    /// Acknowledgement to subscribe
    #[derive(Debug, Clone, PartialEq)]
    pub struct SubAck {
        pub pkid: u16,
        pub return_codes: Vec<SubscribeReasonCode>,
    }

    impl SubAck {
        pub fn new(pkid: u16, return_codes: Vec<SubscribeReasonCode>) -> SubAck {
            SubAck { pkid, return_codes }
        }

        pub fn len(&self) -> usize {
            2 + self.return_codes.len()
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);
            let pkid = read_u16(&mut bytes)?;

            if !bytes.has_remaining() {
                return Err(Error::MalformedPacket);
            }

            let mut return_codes = Vec::new();
            while bytes.has_remaining() {
                let return_code = read_u8(&mut bytes)?;
                return_codes.push(return_code.try_into()?);
            }

            let suback = SubAck { pkid, return_codes };
            Ok(suback)
        }
    }

    pub fn write(
        return_codes: Vec<SubscribeReasonCode>,
        pkid: u16,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        let len = 2 + return_codes.len();
        buffer.put_u8(0x90);

        let remaining_len_bytes = write_remaining_length(buffer, len)?;
        buffer.put_u16(pkid);

        let p: Vec<u8> = return_codes
            .iter()
            .map(|&code| match code {
                SubscribeReasonCode::Success(qos) => qos as u8,
                SubscribeReasonCode::Failure => 0x80,
            })
            .collect();

        buffer.extend_from_slice(&p);
        Ok(1 + remaining_len_bytes + len)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SubscribeReasonCode {
        Success(QoS),
        Failure,
    }

    impl TryFrom<u8> for SubscribeReasonCode {
        type Error = Error;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            let v = match value {
                0 => SubscribeReasonCode::Success(QoS::AtMostOnce),
                1 => SubscribeReasonCode::Success(QoS::AtLeastOnce),
                128 => SubscribeReasonCode::Failure,
                v => return Err(Error::InvalidSubscribeReasonCode(v)),
            };

            Ok(v)
        }
    }

    pub fn codes(c: Vec<u8>) -> Vec<SubscribeReasonCode> {
        c.into_iter()
            .map(|v| {
                let qos = qos(v).unwrap();
                SubscribeReasonCode::Success(qos)
            })
            .collect()
    }
}

pub(crate) mod pingresp {
    use super::*;

    pub fn write(payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xD0, 0x00]);
        Ok(2)
    }
}

pub(crate) mod pingreq {
    use super::*;

    pub fn write(payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xC0, 0x00]);
        Ok(2)
    }
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read_mut(stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = packet.freeze();
    let packet = match packet_type {
        PacketType::Connect => Packet::Connect(connect::Connect::read(fixed_header, packet)?),
        PacketType::ConnAck => Packet::ConnAck(connack::ConnAck::read(fixed_header, packet)?),
        PacketType::Publish => Packet::Publish(publish::Publish::read(fixed_header, packet)?),
        PacketType::PubAck => Packet::PubAck(puback::PubAck::read(fixed_header, packet)?),
        PacketType::Subscribe => {
            Packet::Subscribe(subscribe::Subscribe::read(fixed_header, packet)?)
        }
        PacketType::SubAck => Packet::SubAck(suback::SubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
        v => return Err(Error::UnsupportedPacket(v)),
    };

    Ok(packet)
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read(stream: &mut Bytes, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = match packet_type {
        PacketType::Connect => Packet::Connect(connect::Connect::read(fixed_header, packet)?),
        PacketType::ConnAck => Packet::ConnAck(connack::ConnAck::read(fixed_header, packet)?),
        PacketType::Publish => Packet::Publish(publish::Publish::read(fixed_header, packet)?),
        PacketType::PubAck => Packet::PubAck(puback::PubAck::read(fixed_header, packet)?),
        PacketType::Subscribe => {
            Packet::Subscribe(subscribe::Subscribe::read(fixed_header, packet)?)
        }
        PacketType::SubAck => Packet::SubAck(suback::SubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
        v => return Err(Error::UnsupportedPacket(v)),
    };

    Ok(packet)
}

/// MQTT packet type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect = 1,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Packet {
    Connect(connect::Connect),
    Publish(publish::Publish),
    ConnAck(connack::ConnAck),
    PubAck(puback::PubAck),
    PingReq,
    PingResp,
    Subscribe(subscribe::Subscribe),
    SubAck(suback::SubAck),
    Disconnect,
}

/// Quality of service
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
}

/// Maps a number to QoS
pub fn qos(num: u8) -> Result<QoS, Error> {
    match num {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        qos => Err(Error::InvalidQoS(qos)),
    }
}

/// Packet type from a byte
///
/// ```ignore
///          7                          3                          0
///          +--------------------------+--------------------------+
/// byte 1   | MQTT Control Packet Type | Flags for each type      |
///          +--------------------------+--------------------------+
///          |         Remaining Bytes Len  (1/2/3/4 bytes)        |
///          +-----------------------------------------------------+
///
/// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_2.2_-
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct FixedHeader {
    /// First byte of the stream. Used to identify packet types and
    /// several flags
    pub byte1: u8,
    /// Length of fixed header. Byte 1 + (1..4) bytes. So fixed header
    /// len can vary from 2 bytes to 5 bytes
    /// 1..4 bytes are variable length encoded to represent remaining length
    pub fixed_header_len: usize,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    pub remaining_len: usize,
}

impl FixedHeader {
    pub fn new(byte1: u8, remaining_len_len: usize, remaining_len: usize) -> FixedHeader {
        FixedHeader {
            byte1,
            fixed_header_len: remaining_len_len + 1,
            remaining_len,
        }
    }

    pub fn packet_type(&self) -> Result<PacketType, Error> {
        let num = self.byte1 >> 4;
        match num {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::ConnAck),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::PubAck),
            5 => Ok(PacketType::PubRec),
            6 => Ok(PacketType::PubRel),
            7 => Ok(PacketType::PubComp),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::SubAck),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::UnsubAck),
            12 => Ok(PacketType::PingReq),
            13 => Ok(PacketType::PingResp),
            14 => Ok(PacketType::Disconnect),
            _ => Err(Error::InvalidPacketType(num)),
        }
    }

    /// Returns the size of full packet (fixed header + variable header + payload)
    /// Fixed header is enough to get the size of a frame in the stream
    pub fn frame_length(&self) -> usize {
        self.fixed_header_len + self.remaining_len
    }
}

/// Checks if the stream has enough bytes to frame a packet and returns fixed header
/// only if a packet can be framed with existing bytes in the `stream`.
/// The passed stream doesn't modify parent stream's cursor. If this function
/// returned an error, next `check` on the same parent stream is forced start
/// with cursor at 0 again (Iter is owned. Only Iter's cursor is changed internally)
pub fn check(stream: Iter<u8>, max_packet_size: usize) -> Result<FixedHeader, Error> {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.len();
    let fixed_header = parse_fixed_header(stream)?;

    // Don't let rogue connections attack with huge payloads.
    // Disconnect them before reading all that data
    if fixed_header.remaining_len > max_packet_size {
        return Err(Error::PayloadSizeLimitExceeded(fixed_header.remaining_len));
    }

    // If the current call fails due to insufficient bytes in the stream,
    // after calculating remaining length, we extend the stream
    let frame_length = fixed_header.frame_length();
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    Ok(fixed_header)
}

/// Parses fixed header
fn parse_fixed_header(mut stream: Iter<u8>) -> Result<FixedHeader, Error> {
    // At least 2 bytes are necessary to frame a packet
    let stream_len = stream.len();
    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream.next().unwrap();
    let (len_len, len) = length(stream)?;

    Ok(FixedHeader::new(*byte1, len_len, len))
}

/// Parses variable byte integer in the stream and returns the length
/// and number of bytes that make it. Used for remaining length calculation
/// as well as for calculating property lengths
pub fn length(stream: Iter<u8>) -> Result<(usize, usize), Error> {
    let mut len: usize = 0;
    let mut len_len = 0;
    let mut done = false;
    let mut shift = 0;

    // Use continuation bit at position 7 to continue reading next
    // byte to frame 'length'.
    // Stream 0b1xxx_xxxx 0b1yyy_yyyy 0b1zzz_zzzz 0b0www_wwww will
    // be framed as number 0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
    for byte in stream {
        len_len += 1;
        let byte = *byte as usize;
        len += (byte & 0x7F) << shift;

        // stop when continue bit is 0
        done = (byte & 0x80) == 0;
        if done {
            break;
        }

        shift += 7;

        // Only a max of 4 bytes allowed for remaining length
        // more than 4 shifts (0, 7, 14, 21) implies bad length
        if shift > 21 {
            return Err(Error::MalformedRemainingLength);
        }
    }

    // Not enough bytes to frame remaining length. wait for
    // one more byte
    if !done {
        return Err(Error::InsufficientBytes(1));
    }

    Ok((len_len, len))
}

/// Returns big endian u16 view from next 2 bytes
pub fn view_u16(stream: &[u8]) -> Result<u16, Error> {
    let v = match stream.get(0..2) {
        Some(v) => (v[0] as u16) << 8 | (v[1] as u16),
        None => return Err(Error::MalformedPacket),
    };

    Ok(v)
}

/// Returns big endian u16 view from next 2 bytes
pub fn view_str(stream: &[u8], end: usize) -> Result<&str, Error> {
    let v = match stream.get(0..end) {
        Some(v) => v,
        None => return Err(Error::BoundaryCrossed(stream.len())),
    };

    let v = std::str::from_utf8(v)?;
    Ok(v)
}

/// After collecting enough bytes to frame a packet (packet's frame())
/// , It's possible that content itself in the stream is wrong. Like expected
/// packet id or qos not being present. In cases where `read_mqtt_string` or
/// `read_mqtt_bytes` exhausted remaining length but packet framing expects to
/// parse qos next, these pre checks will prevent `bytes` crashes
pub fn read_u16(stream: &mut Bytes) -> Result<u16, Error> {
    if stream.len() < 2 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u16())
}

fn read_u8(stream: &mut Bytes) -> Result<u8, Error> {
    if stream.is_empty() {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u8())
}

/// Reads a series of bytes with a length from a byte stream
fn read_mqtt_bytes(stream: &mut Bytes) -> Result<Bytes, Error> {
    let len = read_u16(stream)? as usize;

    // Prevent attacks with wrong remaining length. This method is used in
    // `packet.assembly()` with (enough) bytes to frame packet. Ensures that
    // reading variable len string or bytes doesn't cross promised boundary
    // with `read_fixed_header()`
    if len > stream.len() {
        return Err(Error::BoundaryCrossed(len));
    }

    Ok(stream.split_to(len))
}

/// Serializes bytes to stream (including length)
fn write_mqtt_bytes(stream: &mut BytesMut, bytes: &[u8]) {
    stream.put_u16(bytes.len() as u16);
    stream.extend_from_slice(bytes);
}

/// Serializes a string to stream
pub fn write_mqtt_string(stream: &mut BytesMut, string: &str) {
    write_mqtt_bytes(stream, string.as_bytes());
}

/// Writes remaining length to stream and returns number of bytes for remaining length
pub fn write_remaining_length(stream: &mut BytesMut, len: usize) -> Result<usize, Error> {
    if len > 268_435_455 {
        return Err(Error::PayloadTooLong);
    }

    let mut done = false;
    let mut x = len;
    let mut count = 0;

    while !done {
        let mut byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            byte |= 128;
        }

        stream.put_u8(byte);
        count += 1;
        done = x == 0;
    }

    Ok(count)
}

/// Return number of remaining length bytes required for encoding length
fn _len_len(len: usize) -> usize {
    if len >= 2_097_152 {
        4
    } else if len >= 16_384 {
        3
    } else if len >= 128 {
        2
    } else {
        1
    }
}

/// Error during serialization and deserialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Expected connect packet, received = {0:?}")]
    NotConnect(PacketType),
    #[error("Received an unexpected connect packet")]
    UnexpectedConnect,
    #[error("Invalid return code received as response for connect = {0}")]
    InvalidConnectReturnCode(u8),
    #[error("Invalid reason = {0}")]
    InvalidReason(u8),
    #[error("Invalid protocol used")]
    InvalidProtocol,
    #[error("Invalid protocol level")]
    InvalidProtocolLevel(u8),
    #[error("Invalid packet format")]
    IncorrectPacketFormat,
    #[error("Invalid packet type = {0}")]
    InvalidPacketType(u8),
    #[error("Packet type unsupported = {0:?}")]
    UnsupportedPacket(PacketType),
    #[error("Invalid retain forward rule = {0}")]
    InvalidRetainForwardRule(u8),
    #[error("Invalid QoS level = {0}")]
    InvalidQoS(u8),
    #[error("Invalid subscribe reason code = {0}")]
    InvalidSubscribeReasonCode(u8),
    #[error("Packet received has id Zero")]
    PacketIdZero,
    #[error("Subscription had id Zero")]
    SubscriptionIdZero,
    #[error("Payload size is incorrect")]
    PayloadSizeIncorrect,
    #[error("Payload is too long")]
    PayloadTooLong,
    #[error("Payload size has been exceeded by {0} bytes")]
    PayloadSizeLimitExceeded(usize),
    #[error("Payload is required")]
    PayloadRequired,
    #[error("Topic not utf-8 = {0}")]
    TopicNotUtf8(#[from] Utf8Error),
    #[error("Promised boundary crossed, contains {0} bytes")]
    BoundaryCrossed(usize),
    #[error("Packet is malformed")]
    MalformedPacket,
    #[error("Remaining length is malformed")]
    MalformedRemainingLength,
    /// More bytes required to frame packet. Argument
    /// implies minimum additional bytes required to
    /// proceed further
    #[error("Insufficient number of bytes to frame packet, {0} more bytes required")]
    InsufficientBytes(usize),
}
