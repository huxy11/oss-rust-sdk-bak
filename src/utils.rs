use super::errors::Error;
use reqwest::header::{HeaderMap, HeaderName};
use std::collections::HashMap;

pub(crate) const OSS_META_PREFIX: &str = "x-oss-meta-";

pub fn to_meta_headers<S>(meta: HashMap<S, S>) -> Result<HeaderMap, Error>
where
    S: AsRef<str>,
{
    let mut headers = HeaderMap::new();
    for (key, val) in meta.iter() {
        headers.insert(
            HeaderName::from_bytes(
                &OSS_META_PREFIX
                    .bytes()
                    .chain(key.as_ref().bytes())
                    .collect::<Vec<_>>(),
            )?,
            val.as_ref().parse()?,
        );
    }
    Ok(headers)
}
pub fn to_headers<S>(hashmap: HashMap<S, S>) -> Result<HeaderMap, Error>
where
    S: AsRef<str>,
{
    let mut headers = HeaderMap::new();
    for (key, val) in hashmap.iter() {
        headers.insert(
            HeaderName::from_bytes(key.as_ref().as_bytes())?,
            val.as_ref().parse()?,
        );
    }
    Ok(headers)
}
