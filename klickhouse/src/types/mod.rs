use std::str::FromStr;

use anyhow::*;
use chrono_tz::Tz;
use uuid::Uuid;

mod deserialize;
mod low_cardinality;
mod serialize;
#[cfg(test)]
mod tests;

use crate::{
    i256,
    io::{ClickhouseRead, ClickhouseWrite},
    u256,
    values::Value,
    Date, DateTime, Ipv4, Ipv6,
};

/// A raw Clickhouse type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    Float32,
    Float64,

    Decimal32(usize),
    Decimal64(usize),
    Decimal128(usize),
    Decimal256(usize),

    String,
    FixedString(usize),

    Uuid,

    Date,
    DateTime(Tz),
    DateTime64(usize, Tz),

    Ipv4,
    Ipv6,

    /// Not supported
    Enum8(Vec<(String, u8)>),
    /// Not supported
    Enum16(Vec<(String, u16)>),

    LowCardinality(Box<Type>),

    Array(Box<Type>),

    // unused (server never sends this)
    // Nested(IndexMap<String, Type>),
    Tuple(Vec<Type>),

    Nullable(Box<Type>),

    Map(Box<Type>, Box<Type>),
}

impl Type {
    pub fn unwrap_array(&self) -> &Type {
        match self {
            Type::Array(x) => &**x,
            _ => unimplemented!(),
        }
    }

    pub fn unwrap_map(&self) -> (&Type, &Type) {
        match self {
            Type::Map(key, value) => (&**key, &**value),
            _ => unimplemented!(),
        }
    }

    pub fn unwrap_tuple(&self) -> &[Type] {
        match self {
            Type::Tuple(x) => &x[..],
            _ => unimplemented!(),
        }
    }

    pub fn strip_null(&self) -> &Type {
        match self {
            Type::Nullable(x) => &**x,
            _ => self,
        }
    }

    pub fn is_nullable(&self) -> bool {
        matches!(self, Type::Nullable(_))
    }

    pub fn default_value(&self) -> Value {
        match self {
            Type::Int8 => Value::Int8(0),
            Type::Int16 => Value::Int16(0),
            Type::Int32 => Value::Int32(0),
            Type::Int64 => Value::Int64(0),
            Type::Int128 => Value::Int128(0),
            Type::Int256 => Value::Int256(i256::default()),
            Type::UInt8 => Value::UInt8(0),
            Type::UInt16 => Value::UInt16(0),
            Type::UInt32 => Value::UInt32(0),
            Type::UInt64 => Value::UInt64(0),
            Type::UInt128 => Value::UInt128(0),
            Type::UInt256 => Value::UInt256(u256::default()),
            Type::Float32 => Value::Float32(0),
            Type::Float64 => Value::Float64(0),
            Type::Decimal32(s) => Value::Decimal32(*s, 0),
            Type::Decimal64(s) => Value::Decimal64(*s, 0),
            Type::Decimal128(s) => Value::Decimal128(*s, 0),
            Type::Decimal256(s) => Value::Decimal256(*s, i256::default()),
            Type::String => Value::String("".to_string()),
            Type::FixedString(_) => Value::String("".to_string()),
            Type::Uuid => Value::Uuid(Uuid::from_u128(0)),
            Type::Date => Value::Date(Date(0)),
            Type::DateTime(tz) => Value::DateTime(DateTime(*tz, 0)),
            Type::DateTime64(precision, tz) => Value::DateTime64(*tz, *precision, 0),
            Type::Ipv4 => Value::Ipv4(Ipv4::default()),
            Type::Ipv6 => Value::Ipv6(Ipv6::default()),
            Type::Enum8(_) => Value::Enum8(0),
            Type::Enum16(_) => Value::Enum16(0),
            Type::LowCardinality(x) => x.default_value(),
            Type::Array(_) => Value::Array(vec![]),
            // Type::Nested(_) => unimplemented!(),
            Type::Tuple(types) => Value::Tuple(types.iter().map(|x| x.default_value()).collect()),
            Type::Nullable(_) => Value::Null,
            Type::Map(_, _) => Value::Map(vec![], vec![]),
        }
    }

    pub fn strip_low_cardinality(&self) -> &Type {
        match self {
            Type::LowCardinality(x) => &**x,
            _ => self,
        }
    }
}

// we assume complete identifier normalization and type resolution from clickhouse
fn eat_identifier(input: &str) -> (&str, &str) {
    for (i, c) in input.char_indices() {
        if c.is_alphabetic() || c == '_' || c == '$' || (i > 0 && c.is_numeric()) {
            continue;
        } else {
            return (&input[..i], &input[i..]);
        }
    }
    (input, "")
}

fn parse_args(input: &str) -> Result<Vec<&str>> {
    if !input.starts_with('(') || !input.ends_with(')') {
        return Err(anyhow!("malformed arguments to type"));
    }
    let input = input[1..input.len() - 1].trim();
    let mut out = vec![];
    let mut in_parens = 0usize;
    let mut last_start = 0;
    // todo: handle parens in enum strings?
    for (i, c) in input.char_indices() {
        match c {
            ',' => {
                if in_parens == 0 {
                    out.push(input[last_start..i].trim());
                    last_start = i + 1;
                }
            }
            '(' => {
                in_parens += 1;
            }
            ')' => {
                in_parens -= 1;
            }
            _ => (),
        }
    }
    if in_parens != 0 {
        return Err(anyhow!("mismatched parenthesis"));
    }
    if last_start != input.len() {
        out.push(input[last_start..input.len()].trim());
    }
    Ok(out)
}

impl FromStr for Type {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let (ident, following) = eat_identifier(s);
        if ident.is_empty() {
            return Err(anyhow!("invalid empty identifier for type: '{}'", s));
        }
        let following = following.trim();
        if !following.is_empty() {
            let args = parse_args(following)?;
            return Ok(match ident {
                "Decimal" => {
                    if args.len() != 2 {
                        return Err(anyhow!("bad arg count for Decimal"));
                    }
                    let p: usize = args[0].parse()?;
                    let s: usize = args[1].parse()?;
                    if p <= 9 {
                        Type::Decimal32(s)
                    } else if p <= 18 {
                        Type::Decimal64(s)
                    } else if p <= 38 {
                        Type::Decimal128(s)
                    } else if p <= 76 {
                        Type::Decimal256(s)
                    } else {
                        return Err(anyhow!("bad decimal spec"));
                    }
                }
                "Decimal32" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for Decimal32"));
                    }
                    Type::Decimal32(args[0].parse()?)
                }
                "Decimal64" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for Decimal64"));
                    }
                    Type::Decimal64(args[0].parse()?)
                }
                "Decimal128" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for Decimal128"));
                    }
                    Type::Decimal128(args[0].parse()?)
                }
                "Decimal256" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for Decimal256"));
                    }
                    Type::Decimal256(args[0].parse()?)
                }
                "FixedString" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for FixedString"));
                    }
                    Type::FixedString(args[0].parse()?)
                }
                "DateTime" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for DateTime"));
                    }
                    if !args[0].starts_with('\'') || !args[0].ends_with('\'') {
                        return Err(anyhow!("failed to parse timezone for DateTime"));
                    }
                    Type::DateTime(
                        args[0][1..args[0].len() - 1]
                            .parse()
                            .map_err(|_| anyhow!("failed to parse timezone for DateTime"))?,
                    )
                }
                "DateTime64" => {
                    if args.len() == 2 {
                        if !args[1].starts_with('\'') || !args[1].ends_with('\'') {
                            return Err(anyhow!("failed to parse timezone for DateTime64"));
                        }
                        Type::DateTime64(
                            args[0].parse()?,
                            args[1][1..args[1].len() - 1]
                                .parse()
                                .map_err(|_| anyhow!("failed to parse timezone for DateTime64"))?,
                        )
                    } else if args.len() == 1 {
                        Type::DateTime64(args[0].parse()?, chrono_tz::UTC)
                    } else {
                        return Err(anyhow!("bad arg count for DateTime64"));
                    }
                }
                "Enum8" => {
                    todo!()
                }
                "Enum16" => {
                    todo!()
                }
                "LowCardinality" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for LowCardinality"));
                    }
                    Type::LowCardinality(Box::new(Type::from_str(args[0])?))
                }
                "Array" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for Array"));
                    }
                    Type::Array(Box::new(Type::from_str(args[0])?))
                }
                "Nested" => {
                    todo!()
                }
                "Tuple" => {
                    let mut inner = vec![];
                    for arg in args {
                        inner.push(arg.trim().parse()?);
                    }
                    Type::Tuple(inner)
                }
                "Nullable" => {
                    if args.len() != 1 {
                        return Err(anyhow!("bad arg count for Nullable"));
                    }
                    Type::Nullable(Box::new(Type::from_str(args[0])?))
                }
                "Map" => {
                    if args.len() != 2 {
                        return Err(anyhow!("bad arg count for Map"));
                    }
                    Type::Map(
                        Box::new(Type::from_str(args[0])?),
                        Box::new(Type::from_str(args[1])?),
                    )
                }
                _ => return Err(anyhow!("invalid type with arguments: '{}'", ident)),
            });
        }
        Ok(match ident {
            "Int8" => Type::Int8,
            "Int16" => Type::Int16,
            "Int32" => Type::Int32,
            "Int64" => Type::Int64,
            "Int128" => Type::Int128,
            "Int256" => Type::Int256,
            "UInt8" => Type::UInt8,
            "UInt16" => Type::UInt16,
            "UInt32" => Type::UInt32,
            "UInt64" => Type::UInt64,
            "UInt128" => Type::UInt128,
            "UInt256" => Type::UInt256,
            "Float32" => Type::Float32,
            "Float64" => Type::Float64,
            "String" => Type::String,
            "UUID" => Type::Uuid,
            "Date" => Type::Date,
            "DateTime" => Type::DateTime(chrono_tz::UTC),
            "IPv4" => Type::Ipv4,
            "IPv6" => Type::Ipv6,
            _ => return Err(anyhow!("invalid type name: '{}'", ident)),
        })
    }
}

impl ToString for Type {
    fn to_string(&self) -> String {
        match self {
            Type::Int8 => "Int8".to_string(),
            Type::Int16 => "Int16".to_string(),
            Type::Int32 => "Int32".to_string(),
            Type::Int64 => "Int64".to_string(),
            Type::Int128 => "Int128".to_string(),
            Type::Int256 => "Int256".to_string(),
            Type::UInt8 => "UInt8".to_string(),
            Type::UInt16 => "UInt16".to_string(),
            Type::UInt32 => "UInt32".to_string(),
            Type::UInt64 => "UInt64".to_string(),
            Type::UInt128 => "UInt128".to_string(),
            Type::UInt256 => "UInt256".to_string(),
            Type::Float32 => "Float32".to_string(),
            Type::Float64 => "Float64".to_string(),
            Type::Decimal32(s) => format!("Decimal32({})", s),
            Type::Decimal64(s) => format!("Decimal64({})", s),
            Type::Decimal128(s) => format!("Decimal128({})", s),
            Type::Decimal256(s) => format!("Decimal256({})", s),
            Type::String => "String".to_string(),
            Type::FixedString(s) => format!("FixedString({})", s),
            Type::Uuid => "UUID".to_string(),
            Type::Date => "Date".to_string(),
            Type::DateTime(tz) => format!("DateTime('{}')", tz),
            Type::DateTime64(precision, tz) => format!("DateTime64({},'{}')", precision, tz),
            Type::Ipv4 => "IPv4".to_string(),
            Type::Ipv6 => "IPv6".to_string(),
            Type::Enum8(items) => format!(
                "Enum8({})",
                items
                    .iter()
                    .map(|(name, value)| format!("{}={}", name, value))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Type::Enum16(items) => format!(
                "Enum16({})",
                items
                    .iter()
                    .map(|(name, value)| format!("{}={}", name, value))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Type::LowCardinality(inner) => format!("LowCardinality({})", inner.to_string()),
            Type::Array(inner) => format!("Array({})", inner.to_string()),
            // Type::Nested(items) => format!("Nested({})", items.iter().map(|(key, value)| format!("{} {}", key, value.to_string())).collect::<Vec<_>>().join(",")),
            Type::Tuple(items) => format!(
                "Tuple({})",
                items
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Type::Nullable(inner) => format!("Nullable({})", inner.to_string()),
            Type::Map(key, value) => format!("Map({},{})", key.to_string(), value.to_string()),
        }
    }
}

impl Type {
    pub(crate) async fn deserialize_prefix<R: ClickhouseRead>(
        &self,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        use deserialize::*;
        match self {
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Uuid
            | Type::Date
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Ipv4
            | Type::Ipv6
            | Type::Enum8(_)
            | Type::Enum16(_) => sized::SizedDeserializer::read_prefix(self, reader, state).await?,

            Type::String | Type::FixedString(_) => {
                string::StringDeserializer::read_prefix(self, reader, state).await?
            }

            Type::Array(_) => array::ArrayDeserializer::read_prefix(self, reader, state).await?,
            Type::Tuple(_) => tuple::TupleDeserializer::read_prefix(self, reader, state).await?,
            Type::Nullable(_) => {
                nullable::NullableDeserializer::read_prefix(self, reader, state).await?
            }
            Type::Map(_, _) => map::MapDeserializer::read_prefix(self, reader, state).await?,
            Type::LowCardinality(_) => {
                low_cardinality::LowCardinalityDeserializer::read_prefix(self, reader, state)
                    .await?
            }
        }
        Ok(())
    }

    pub(crate) async fn deserialize_column<R: ClickhouseRead>(
        &self,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        use deserialize::*;
        Ok(match self {
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Uuid
            | Type::Date
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Ipv4
            | Type::Ipv6
            | Type::Enum8(_)
            | Type::Enum16(_) => {
                sized::SizedDeserializer::read_n(self, reader, rows, state).await?
            }

            Type::String | Type::FixedString(_) => {
                string::StringDeserializer::read_n(self, reader, rows, state).await?
            }

            Type::Array(_) => array::ArrayDeserializer::read_n(self, reader, rows, state).await?,
            Type::Tuple(_) => tuple::TupleDeserializer::read_n(self, reader, rows, state).await?,
            Type::Nullable(_) => {
                nullable::NullableDeserializer::read_n(self, reader, rows, state).await?
            }
            Type::Map(_, _) => map::MapDeserializer::read_n(self, reader, rows, state).await?,
            Type::LowCardinality(_) => {
                low_cardinality::LowCardinalityDeserializer::read_n(self, reader, rows, state)
                    .await?
            }
        })
    }

    pub(crate) async fn deserialize<R: ClickhouseRead>(
        &self,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<Value> {
        use deserialize::*;
        Ok(match self {
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Uuid
            | Type::Date
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Ipv4
            | Type::Ipv6
            | Type::Enum8(_)
            | Type::Enum16(_) => sized::SizedDeserializer::read(self, reader, state).await?,

            Type::String | Type::FixedString(_) => {
                string::StringDeserializer::read(self, reader, state).await?
            }

            Type::Array(_) => array::ArrayDeserializer::read(self, reader, state).await?,
            Type::Tuple(_) => tuple::TupleDeserializer::read(self, reader, state).await?,
            Type::Nullable(_) => nullable::NullableDeserializer::read(self, reader, state).await?,
            Type::Map(_, _) => map::MapDeserializer::read(self, reader, state).await?,
            Type::LowCardinality(_) => {
                low_cardinality::LowCardinalityDeserializer::read(self, reader, state).await?
            }
        })
    }

    pub(crate) async fn serialize_column<W: ClickhouseWrite>(
        &self,
        values: &[Value],
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        use serialize::*;
        match self {
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Uuid
            | Type::Date
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Ipv4
            | Type::Ipv6
            | Type::Enum8(_)
            | Type::Enum16(_) => {
                sized::SizedSerializer::write_n(self, values, writer, state).await?
            }

            Type::String | Type::FixedString(_) => {
                string::StringSerializer::write_n(self, values, writer, state).await?
            }

            Type::Array(_) => array::ArraySerializer::write_n(self, values, writer, state).await?,
            Type::Tuple(_) => tuple::TupleSerializer::write_n(self, values, writer, state).await?,
            Type::Nullable(_) => {
                nullable::NullableSerializer::write_n(self, values, writer, state).await?
            }
            Type::Map(_, _) => map::MapSerializer::write_n(self, values, writer, state).await?,
            Type::LowCardinality(_) => {
                low_cardinality::LowCardinalitySerializer::write_n(self, values, writer, state)
                    .await?
            }
        }
        Ok(())
    }

    pub(crate) async fn serialize<W: ClickhouseWrite>(
        &self,
        value: &Value,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        use serialize::*;
        match self {
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Uuid
            | Type::Date
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Ipv4
            | Type::Ipv6
            | Type::Enum8(_)
            | Type::Enum16(_) => sized::SizedSerializer::write(self, value, writer, state).await?,

            Type::String | Type::FixedString(_) => {
                string::StringSerializer::write(self, value, writer, state).await?
            }

            Type::Array(_) => array::ArraySerializer::write(self, value, writer, state).await?,
            Type::Tuple(_) => tuple::TupleSerializer::write(self, value, writer, state).await?,
            Type::Nullable(_) => {
                nullable::NullableSerializer::write(self, value, writer, state).await?
            }
            Type::Map(_, _) => map::MapSerializer::write(self, value, writer, state).await?,
            Type::LowCardinality(_) => {
                low_cardinality::LowCardinalitySerializer::write(self, value, writer, state).await?
            }
        }
        Ok(())
    }

    pub(crate) async fn serialize_prefix<W: ClickhouseWrite>(
        &self,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        use serialize::*;
        match self {
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Uuid
            | Type::Date
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Ipv4
            | Type::Ipv6
            | Type::Enum8(_)
            | Type::Enum16(_) => sized::SizedSerializer::write_prefix(self, writer, state).await?,

            Type::String | Type::FixedString(_) => {
                string::StringSerializer::write_prefix(self, writer, state).await?
            }

            Type::Array(_) => array::ArraySerializer::write_prefix(self, writer, state).await?,
            Type::Tuple(_) => tuple::TupleSerializer::write_prefix(self, writer, state).await?,
            Type::Nullable(_) => {
                nullable::NullableSerializer::write_prefix(self, writer, state).await?
            }
            Type::Map(_, _) => map::MapSerializer::write_prefix(self, writer, state).await?,
            Type::LowCardinality(_) => {
                low_cardinality::LowCardinalitySerializer::write_prefix(self, writer, state).await?
            }
        }
        Ok(())
    }

    pub(crate) fn validate(&self, dimensions: usize) -> Result<()> {
        match self {
            Type::Decimal32(precision) => {
                if *precision == 0 || *precision > 9 {
                    return Err(anyhow!(
                        "precision out of bounds for Decimal32({}) must be in range (1..=9)",
                        *precision
                    ));
                }
            }
            Type::DateTime64(precision, _) | Type::Decimal64(precision) => {
                if *precision == 0 || *precision > 18 {
                    return Err(anyhow!("precision out of bounds for Decimal64/DateTime64({}) must be in range (1..=18)", *precision));
                }
            }
            Type::Decimal128(precision) => {
                if *precision == 0 || *precision > 38 {
                    return Err(anyhow!(
                        "precision out of bounds for Decimal128({}) must be in range (1..=38)",
                        *precision
                    ));
                }
            }
            Type::Decimal256(precision) => {
                if *precision == 0 || *precision > 9 {
                    return Err(anyhow!(
                        "precision out of bounds for Decimal256({}) must be in range (1..=76)",
                        *precision
                    ));
                }
            }
            Type::LowCardinality(inner) => match inner.strip_null() {
                Type::String
                | Type::FixedString(_)
                | Type::Date
                | Type::DateTime(_)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256 => inner.validate(dimensions)?,
                _ => {
                    return Err(anyhow!(
                        "illegal type '{:?}' in LowCardinality, not allowed",
                        inner
                    ))
                }
            },
            Type::Array(inner) => {
                if dimensions >= 2 {
                    return Err(anyhow!("too many dimensions (limited to 2D structure)"));
                }
                inner.validate(dimensions + 1)?;
            }
            // Type::Nested(_) => return Err(anyhow!("nested not implemented")),
            Type::Tuple(inner) => {
                for inner in inner {
                    inner.validate(dimensions)?;
                }
            }
            Type::Nullable(inner) => {
                match &**inner {
                    Type::Array(_)
                    | Type::Map(_, _)
                    | Type::LowCardinality(_)
                    | Type::Tuple(_)
                    | Type::Nullable(_) => {
                        /*  | Type::Nested(_) */
                        return Err(anyhow!(
                            "nullable cannot contain composite type '{:?}'",
                            inner
                        ));
                    }
                    _ => inner.validate(dimensions)?,
                }
            }
            Type::Map(key, value) => {
                if dimensions >= 2 {
                    return Err(anyhow!("too many dimensions (limited to 2D structure)"));
                }
                if !matches!(
                    &**key,
                    Type::String
                        | Type::FixedString(_)
                        | Type::Int8
                        | Type::Int16
                        | Type::Int32
                        | Type::Int64
                        | Type::Int128
                        | Type::Int256
                        | Type::UInt8
                        | Type::UInt16
                        | Type::UInt32
                        | Type::UInt64
                        | Type::UInt128
                        | Type::UInt256
                ) {
                    return Err(anyhow!(
                        "key in map must be String, FixedString(n), or integer"
                    ));
                }
                key.validate(dimensions + 1)?;
                if !matches!(
                    &**value,
                    Type::String
                        | Type::FixedString(_)
                        | Type::Int8
                        | Type::Int16
                        | Type::Int32
                        | Type::Int64
                        | Type::Int128
                        | Type::Int256
                        | Type::UInt8
                        | Type::UInt16
                        | Type::UInt32
                        | Type::UInt64
                        | Type::UInt128
                        | Type::UInt256
                        | Type::Array(_)
                ) {
                    return Err(anyhow!(
                        "value in map must be String, FixedString(n), integer, or array, given {:#?}", value
                    ));
                }
                value.validate(dimensions + 1)?;
            }
            _ => (),
        }
        Ok(())
    }

    pub(crate) fn validate_value(&self, value: &Value) -> Result<()> {
        self.validate(0)?;
        if !self.inner_validate_value(value) {
            return Err(anyhow!(
                "could not assign value '{:?}' to type '{:?}'",
                value,
                self
            ));
        }
        Ok(())
    }

    fn inner_validate_value(&self, value: &Value) -> bool {
        match (self, value) {
            (Type::Int8, Value::Int8(_))
            | (Type::Int16, Value::Int16(_))
            | (Type::Int32, Value::Int32(_))
            | (Type::Int64, Value::Int64(_))
            | (Type::Int128, Value::Int128(_))
            | (Type::Int256, Value::Int256(_))
            | (Type::UInt8, Value::UInt8(_))
            | (Type::UInt16, Value::UInt16(_))
            | (Type::UInt32, Value::UInt32(_))
            | (Type::UInt64, Value::UInt64(_))
            | (Type::UInt128, Value::UInt128(_))
            | (Type::UInt256, Value::UInt256(_))
            | (Type::Float32, Value::Float32(_))
            | (Type::Float64, Value::Float64(_)) => true,
            (Type::Decimal32(precision1), Value::Decimal32(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal64(precision1), Value::Decimal64(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal128(precision1), Value::Decimal128(precision2, _)) => {
                precision1 == precision2
            }
            (Type::Decimal256(precision1), Value::Decimal256(precision2, _)) => {
                precision1 == precision2
            }
            (Type::String, Value::String(_))
            | (Type::FixedString(_), Value::String(_))
            | (Type::Uuid, Value::Uuid(_))
            | (Type::Date, Value::Date(_)) => true,
            (Type::DateTime(tz1), Value::DateTime(date)) => tz1 == &date.0,
            (Type::DateTime64(precision1, tz1), Value::DateTime64(tz2, precision2, _)) => {
                tz1 == tz2 && precision1 == precision2
            }
            (Type::Ipv4, Value::Ipv4(_)) | (Type::Ipv6, Value::Ipv6(_)) => true,
            (Type::Enum8(entries), Value::Enum8(index)) => entries.iter().any(|x| x.1 == *index),
            (Type::Enum16(entries), Value::Enum16(index)) => entries.iter().any(|x| x.1 == *index),
            (Type::LowCardinality(x), value) => x.inner_validate_value(value),
            (Type::Array(inner_type), Value::Array(values)) => {
                values.iter().all(|x| inner_type.inner_validate_value(x))
            }
            (Type::Tuple(inner_types), Value::Tuple(values)) => inner_types
                .iter()
                .zip(values.iter())
                .all(|(type_, value)| type_.inner_validate_value(value)),
            (Type::Nullable(inner), value) => {
                value == &Value::Null || inner.inner_validate_value(value)
            }
            (Type::Map(key, value), Value::Map(keys, values)) => {
                keys.iter().all(|x| key.inner_validate_value(x))
                    && values.iter().all(|x| value.inner_validate_value(x))
            }
            (_, _) => false,
        }
    }
}

pub struct DeserializerState {}

pub struct SerializerState {}

#[async_trait::async_trait]
pub trait Deserializer {
    async fn read_prefix<R: ClickhouseRead>(
        _type_: &Type,
        _reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<()> {
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<Value>;

    async fn read_n<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        n: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(Self::read(type_, reader, state).await?);
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
pub trait Serializer {
    async fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Ok(())
    }

    async fn write_suffix<W: ClickhouseWrite>(
        _type_: &Type,
        _value: &[Value],
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        value: &Value,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()>;

    async fn write_n<W: ClickhouseWrite>(
        type_: &Type,
        values: &[Value],
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        for value in values {
            Self::write(type_, value, writer, state).await?;
        }
        Ok(())
    }
}
