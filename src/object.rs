use quick_xml::{events::Event, Reader};
use reqwest::header::{HeaderMap, CONTENT_LENGTH, DATE};
use std::collections::{binary_heap::Iter, HashMap};

use super::auth::*;
use super::errors::{Error, ObjectError};
use super::oss::OSS;
use super::utils::*;

pub const NULL_META: &[&str] = &[];

#[derive(Debug, Clone)]
pub struct GetObjResponse {
    pub content: String,
    pub meta: HashMap<String, String>,
    pub headers: HeaderMap,
}

impl GetBufferedObjResponse {
    pub fn new<S: AsRef<str>>(
        content: Vec<u8>,
        headers: HeaderMap,
        meta_filter: &[S],
    ) -> Result<Self, Error> {
        let mut meta = HashMap::new();
        for str in meta_filter {
            if let Some(_value) = headers.get(OSS_META_PREFIX.to_owned() + str.as_ref()) {
                meta.insert(str.as_ref().to_owned(), _value.to_str()?.to_owned());
            }
        }
        let ret = Self {
            content,
            meta,
            headers,
        };
        Ok(ret)
    }
}
#[derive(Debug, Clone, Default)]
pub struct GetBufferedObjResponse {
    pub content: Vec<u8>,
    pub meta: HashMap<String, String>,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, Default)]
pub struct ListDetailsResponse {
    pub is_truncated: bool,
    pub objects: Vec<DetailObjects>,
    pub prefixes: Vec<String>,
    pub next_marker: String,
}

#[derive(Debug, Clone, Default)]
pub struct DetailObjects {
    key: String,
    last_modified: String,
    e_tag: String,
    size: String,
}

#[derive(Debug, Clone)]
pub struct PutOptions<'a> {
    pub content_type: &'a str,
    pub headers: HeaderMap,
    pub params: String,
}
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    pub prefix: String,
    pub marker: String,
    pub delimiter: String,
    pub max_keys: String,
}

pub trait MaxKeys {
    fn into_max_keys(self) -> String;
}

impl MaxKeys for i32 {
    fn into_max_keys(self) -> String {
        self.to_string()
    }
}
impl MaxKeys for &dyn AsRef<str> {
    fn into_max_keys(self) -> String {
        self.as_ref().to_string()
    }
}
impl MaxKeys for &str {
    fn into_max_keys(self) -> String {
        String::from(self)
    }
}
impl MaxKeys for String {
    fn into_max_keys(self) -> String {
        self
    }
}
impl MaxKeys for Option<i32> {
    fn into_max_keys(self) -> String {
        self.unwrap_or(1000).to_string()
    }
}

// impl MaxKeys for Option<AsRef<MaxKeys>> {}
impl ListOptions {
    pub fn new<S1, S2, S3, S4>(prefix: S1, marker: S2, delimiter: S3, max_keys: S4) -> Self
    where
        S1: Into<Option<String>>,
        S2: Into<Option<String>>,
        S3: Into<Option<String>>,
        S4: MaxKeys,
    {
        Self {
            // prefix: prefix.into(),
            // marker: marker.into(),
            // delimiter: delimiter.into(),
            // max_keys: max_keys.to_string(),
            prefix: prefix.into().unwrap_or_default(),
            marker: marker.into().unwrap_or_default(),
            delimiter: delimiter.into().unwrap_or_default(),
            max_keys: max_keys.into_max_keys(),
        }
    }
}

impl<'a> PutOptions<'a> {
    pub fn new<S1, S2, M, H, P>(content_type: &'a S1, meta: M, headers: H, params: P) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        M: Into<Option<HashMap<S2, S2>>>,
        H: Into<Option<HashMap<S2, S2>>>,
        P: Into<Option<HashMap<S2, Option<S2>>>>,
    {
        let mut header_map: HeaderMap = meta
            .into()
            .and_then(|_meta| to_meta_headers(_meta).ok())
            .unwrap_or_default();
        header_map.extend(
            headers
                .into()
                .and_then(|_headers| to_headers(_headers).ok())
                .unwrap_or_default(),
        );
        let param_string = params
            .into()
            .map(|_params| OSS::get_params_str(&_params))
            .unwrap_or_default();
        Self {
            content_type: content_type.as_ref(),
            headers: header_map,
            params: param_string,
        }
    }
}

impl Into<GetObjResponse> for GetBufferedObjResponse {
    fn into(mut self) -> GetObjResponse {
        GetObjResponse {
            content: String::from_utf8(std::mem::take(&mut self.content))
                .expect("Error when converting from utf-8"),
            meta: std::mem::take(&mut self.meta),
            headers: std::mem::take(&mut self.headers),
        }
    }
}

pub trait ObjectAPI {
    fn get<S1, S2, M, P>(
        &self,
        object_name: S1,
        meta_keys: M,
        params: P,
    ) -> Result<GetObjResponse, Error>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: Into<Option<HashMap<S2, Option<S2>>>>,
        M: Into<Vec<S2>>;
    fn get_as_buffer<S1, S2, M, P>(
        &self,
        object_name: S1,
        meta_keys: M,
        params: P,
    ) -> Result<GetBufferedObjResponse, Error>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: Into<Option<HashMap<S2, Option<S2>>>>,
        M: Into<Vec<S2>>;
    fn put<'a, S, O>(&self, buf: &[u8], object_name: S, opts: O) -> Result<(), Error>
    where
        S: AsRef<str>,
        O: Into<Option<&'a PutOptions<'a>>>;

    fn del<S>(&self, object_name: S) -> Result<(), Error>
    where
        S: AsRef<str>;
    fn del_multi<'a, V>(&self, object_name: V) -> Result<(), Error>
    where
        V: Into<Iter<'a, &'a str>>;
    fn head<S>(&self, object_name: S) -> Result<HashMap<String, String>, Error>
    where
        S: AsRef<str>;
    fn list_objects<'a, O>(&self, opts: O) -> Result<Vec<String>, Error>
    where
        O: Into<Option<&'a ListOptions>>;
    fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResponse, Error>
    where
        O: Into<Option<&'a ListOptions>>;
}

impl<'a> ObjectAPI for OSS<'a> {
    fn get<S1, S2, M, P>(
        &self,
        object_name: S1,
        meta_keys: M,
        params: P,
    ) -> Result<GetObjResponse, Error>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: Into<Option<HashMap<S2, Option<S2>>>>,
        M: Into<Vec<S2>>,
    {
        self.get_as_buffer(object_name, meta_keys, params)
            .map(|obj| obj.into())
    }
    fn get_as_buffer<S1, S2, M, P>(
        &self,
        object_name: S1,
        meta_keys: M,
        params: P,
    ) -> Result<GetBufferedObjResponse, Error>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: Into<Option<HashMap<S2, Option<S2>>>>,
        M: Into<Vec<S2>>,
    {
        let object_name = object_name.as_ref();
        let params_string = if let Some(r) = params.into() {
            self.get_resources_str(&r)
        } else {
            String::new()
        };
        let host = self.host(self.bucket(), object_name, &params_string);
        let date = self.date();

        let mut headers = HeaderMap::new();
        headers.insert(DATE, date.parse()?);
        let authorization = self.oss_sign(
            "GET",
            self.key_id(),
            self.key_secret(),
            self.bucket(),
            object_name,
            &params_string,
            &headers,
        );
        headers.insert("Authorization", authorization.parse()?);

        let mut resp = self.client.get(&host).headers(headers).send()?;
        let mut buf: Vec<u8> = vec![];

        if resp.status().is_success() {
            resp.copy_to(&mut buf)?;
            GetBufferedObjResponse::new(buf, resp.headers().to_owned(), &meta_keys.into())
        } else {
            Err(Error::Object(ObjectError::GetError {
                msg: format!("can not get object, status code: {}", resp.status()).into(),
            }))
        }
    }

    fn put<'b, S, O>(&self, buf: &[u8], object_name: S, opts: O) -> Result<(), Error>
    where
        S: AsRef<str>,
        O: Into<Option<&'b PutOptions<'b>>>,
    {
        let object_name = object_name.as_ref();
        let (params, mut headers) = if let Some(_opts) = opts.into() {
            (&_opts.params[..], _opts.headers.clone())
        } else {
            ("", HeaderMap::new())
        };

        let host = self.host(self.bucket(), object_name, &params);
        let date = self.date();

        headers.insert(DATE, date.parse()?);
        headers.insert(CONTENT_LENGTH, buf.len().to_string().parse()?);

        let authorization = self.oss_sign(
            "PUT",
            self.key_id(),
            self.key_secret(),
            self.bucket(),
            object_name,
            &params,
            &headers,
        );
        headers.insert("Authorization", authorization.parse()?);

        let resp = self
            .client
            .put(&host)
            .headers(headers)
            .body(buf.to_owned())
            .send()?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(Error::Object(ObjectError::PutError {
                msg: format!("can not put object, status code: {}", resp.status()).into(),
            }))
        }
    }

    fn del<S>(&self, object_name: S) -> Result<(), Error>
    where
        S: AsRef<str>,
    {
        let object_name = object_name.as_ref();
        let host = self.host(self.bucket(), object_name, "");
        let date = self.date();

        let mut headers = HeaderMap::new();
        headers.insert(DATE, date.parse()?);
        let authorization = self.oss_sign(
            "DELETE",
            self.key_id(),
            self.key_secret(),
            self.bucket(),
            object_name,
            "",
            &headers,
        );
        headers.insert("Authorization", authorization.parse()?);

        let resp = self.client.delete(&host).headers(headers).send()?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(Error::Object(ObjectError::DeleteError {
                msg: format!("can not delete object, status code: {}", resp.status()).into(),
            }))
        }
    }
    fn del_multi<'b, V>(&self, object_names: V) -> Result<(), Error>
    where
        V: Into<Iter<'b, &'b str>>,
    {
        for object_name in object_names.into() {
            self.del(object_name)?;
        }
        Ok(())
    }
    fn head<S>(&self, object_name: S) -> Result<HashMap<String, String>, Error>
    where
        S: AsRef<str>,
    {
        let object_name = object_name.as_ref();
        let host = self.host(self.bucket(), object_name, "");
        let date = self.date();

        let mut headers = HeaderMap::new();
        headers.insert(DATE, date.parse()?);
        let authorization = self.oss_sign(
            "HEAD",
            self.key_id(),
            self.key_secret(),
            self.bucket(),
            object_name,
            "",
            &headers,
        );
        headers.insert("Authorization", authorization.parse()?);

        let resp = self.client.head(&host).headers(headers).send()?;
        if resp.status().is_success() {
            let mut ret = HashMap::new();
            for (key, val) in resp
                .headers()
                .iter()
                .filter(|(k, _)| k.as_str().starts_with("x-oss-meta-"))
            {
                ret.insert(
                    key.as_str().trim_start_matches("x-oss-meta-").to_string(),
                    String::from_utf8(val.as_bytes().to_vec())?,
                );
            }
            Ok(ret)
        } else {
            Err(Error::Object(ObjectError::DeleteError {
                msg: format!("can not delete object, status code: {}", resp.status()).into(),
            }))
        }
    }
    fn list_objects<'b, O>(&self, opts: O) -> Result<Vec<String>, Error>
    where
        O: Into<Option<&'b ListOptions>>,
    {
        let (params_string, oss_resources) =
            OSS::get_list_2_params_str(&opts.into().unwrap_or(&ListOptions::default()));
        let host = self.host(self.bucket(), "", &params_string);
        let date = self.date();

        let mut headers = HeaderMap::new();
        headers.insert(DATE, date.parse()?);
        let authorization = self.oss_sign(
            "GET",
            self.key_id(),
            self.key_secret(),
            self.bucket(),
            "",
            &oss_resources,
            &headers,
        );
        headers.insert("Authorization", authorization.parse()?);

        let resp = self.client.get(&host).headers(headers).send()?;
        let xml_str = resp.text()?;
        let mut result = vec![];
        let mut reader = Reader::from_str(xml_str.as_str());
        let mut buf = Vec::with_capacity(1000);
        reader.trim_text(true);
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => match e.name() {
                    b"Key" => result.push(reader.read_text(e.name(), &mut Vec::new())?),
                    _ => (),
                },
                Ok(Event::Eof) => break,
                Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                _ => (),
            }
            buf.clear();
        }
        Ok(result)
    }
    fn list_details<'b, O>(&self, opts: O) -> Result<ListDetailsResponse, Error>
    where
        O: Into<Option<&'b ListOptions>>,
    {
        let (params_string, oss_resources) =
            OSS::get_list_2_params_str(&opts.into().unwrap_or(&ListOptions::default()));

        let host = self.host(self.bucket(), "", &params_string);
        let date = self.date();

        let mut headers = HeaderMap::new();
        headers.insert(DATE, date.parse()?);
        let authorization = self.oss_sign(
            "GET",
            self.key_id(),
            self.key_secret(),
            self.bucket(),
            "",
            &oss_resources,
            &headers,
        );
        headers.insert("Authorization", authorization.parse()?);

        let resp = self.client.get(&host).headers(headers).send()?;
        let xml_str = resp.text()?;
        println!("{}", xml_str);
        let mut result = ListDetailsResponse::default();
        let mut reader = Reader::from_str(xml_str.as_str());
        let mut buf = Vec::with_capacity(1000);
        let mut cur_obj = DetailObjects::default();
        reader.trim_text(true);
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => match e.name() {
                    b"Contents" => {}
                    b"Key" => cur_obj.key = reader.read_text(e.name(), &mut Vec::new())?,
                    b"LastModified" => {
                        cur_obj.last_modified = reader.read_text(e.name(), &mut Vec::new())?
                    }
                    b"ETag" => cur_obj.e_tag = reader.read_text(e.name(), &mut Vec::new())?,
                    b"Size" => cur_obj.size = reader.read_text(e.name(), &mut Vec::new())?,
                    b"IsTruncated" => {
                        result.is_truncated =
                            reader.read_text(e.name(), &mut Vec::new())?.parse()?
                    }
                    b"NextContinuationToken" => {
                        result.next_marker = reader.read_text(e.name(), &mut Vec::new())?
                    }
                    b"CommonPrefixes" => {
                        let mut buf = Vec::new();
                        loop {
                            match reader.read_event(&mut buf) {
                                Ok(Event::Start(ref e)) => match e.name() {
                                    b"PreFix" => result
                                        .prefixes
                                        .push(reader.read_text(e.name(), &mut Vec::new())?),
                                    _ => {}
                                },
                                Ok(Event::End(ref e)) => match e.name() {
                                    b"CommonPrefixes" => break,
                                    _ => {}
                                },
                                _ => panic!(
                                    "Error at position {}: {:?}",
                                    reader.buffer_position(),
                                    e
                                ),
                            }
                        }
                    }
                    _ => (),
                },
                Ok(Event::End(ref e)) => match e.name() {
                    b"Contents" => {
                        result.objects.push(std::mem::take(&mut cur_obj));
                    }
                    _ => (),
                },
                Ok(Event::Eof) => break,
                Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                _ => (),
            }
            buf.clear();
        }
        Ok(result)
    }
}
