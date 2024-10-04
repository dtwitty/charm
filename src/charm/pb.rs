use prost::Message;
use serde::Deserialize;
use serde::Serialize;

tonic::include_proto!("charmpb");

macro_rules! impl_serialize {
    ($type:ty) => {
        impl Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let bytes = self.encode_to_vec();
                serializer.serialize_bytes(&bytes)
            }
        }
    };
}

macro_rules! impl_deserialize {
    ($type:ty) => {
        impl<'de> Deserialize<'de> for $type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
                let slice = bytes.as_slice();
                Self::decode(slice).map_err(serde::de::Error::custom)
            }
        }
    };
}

macro_rules! impl_serialize_deserialize {
    ($type:ty) => {
        impl_serialize!($type);
        impl_deserialize!($type);
    };
}

impl_serialize_deserialize!(GetRequest);
impl_serialize_deserialize!(PutRequest);
impl_serialize_deserialize!(DeleteRequest);
