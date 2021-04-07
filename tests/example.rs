use std::collections::HashMap;

use oss_rust_sdk::{errors::Error, prelude::*};
use tokio::runtime::Runtime;
const FILE_NAME: &str = "rust_oss_sdk_test";
const FILE_NAMES: &[&str] = &[
    "rust_oss_sdk_test_1",
    "rust_oss_sdk_test_2",
    "rust_oss_sdk_test_3",
    "rust_oss_sdk_test_4",
    "rust_oss_sdk_test_5",
];
const BUF: &[u8] = "This is just a put test".as_bytes();
const BUFS: &[&[u8]] = &[
    "An".as_bytes(),
    "Array".as_bytes(),
    "Of".as_bytes(),
    "Put".as_bytes(),
    "Test".as_bytes(),
];

#[test]
fn smoke_test() {
    let oss_instance = default_oss();
    let mut meta = HashMap::new();
    meta.insert("test-meta-key", "test-meta-val");

    // put
    let opts = PutOptions::new(&"text/plain", meta, None, None);
    let ret = oss_instance.put(BUF, FILE_NAME, &opts);
    assert!(ret.is_ok());
    //signiture_url
    let ret = oss_instance.signiture_url("rust_oss_sdk_test", None, "GET");
    assert!(ret.is_ok() && get_through_sign_url(&&oss_instance, &ret.unwrap()).is_ok());

    //head
    let ret = oss_instance.head(FILE_NAME);
    assert!(ret.is_ok() && ret.unwrap().contains_key("test-meta-key"));
    //get
    let ret = oss_instance.get(FILE_NAME, vec!["length"], None);
    assert!(ret.is_ok());
    //get_as_buffer
    let ret = oss_instance.get_as_buffer(FILE_NAME, vec!["test-meta-key"], None);
    assert!(ret.is_ok() && ret.unwrap().meta.contains_key("test-meta-key"));
    //del
    let ret = oss_instance.del(FILE_NAME);
    assert!(ret.is_ok());
    //check
    let ret = oss_instance.get(FILE_NAME, vec!["test-meta-key"], None);
    assert!(ret.is_err());
    //batch put
    for (obj, buf) in FILE_NAMES.iter().zip(BUFS.iter()) {
        let ret = oss_instance.put(buf, obj, None);
        assert!(ret.is_ok());
        let ret = oss_instance.get(obj, vec!["x-oss-meta-test-meta-key"], None);
        assert!(ret.is_ok() && !ret.unwrap().meta.contains_key("x-oss-meta-test-meta-key"));
    }
    //list_objects
    let mut opts = ListOptions::new("rust_oss_sdk_test".to_string(), None, None, None);
    let ret = oss_instance.list_objects(&opts);
    assert!(ret.is_ok() && ret.unwrap().len() == 5);
    //list_details
    opts.max_keys = 2.to_string();
    let ret = oss_instance.list_details(&opts);
    assert!(ret.is_ok() && ret.as_ref().unwrap().objects.len() == 2);
    //continuous list_details
    opts.marker = ret.unwrap().next_marker;
    opts.max_keys = 5.to_string();
    let ret = oss_instance.list_details(&opts);
    assert!((ret.is_ok() && ret.as_ref().unwrap().objects.len() == 3));

    //batch del
    for obj in FILE_NAMES.iter() {
        let ret = oss_instance.del(obj);
        assert!(ret.is_ok());
    }

    //check
    for obj in FILE_NAMES.iter() {
        // 这里meta_keys 参数没办法继续传一个None 进去, 也没办法简单地传 vec![]或者[]。 因为不知道它的具体类型
        // 会推断不出S2的类型。 先这么做。
        let ret = oss_instance.get(obj, NULL_META, None);
        assert!(ret.is_err());
    }
}

#[test]
fn host_test() {
    let oss_instance = default_oss();
    let s = oss_instance.host(
        oss_instance.bucket(),
        "test.txt",
        "response-content-type=text&response-cache-control=No-cache",
    );
    println!("{:?}", s);
}

#[test]
fn list_object_test() {
    let oss_instance = default_oss();
    let s = oss_instance.list_bucket::<&str, _>(None);
    println!("{:#?}", s);
}

fn default_oss() -> OSS<'static> {
    OSS::new(
        std::env::var("OSS_ID").expect("$OSS_ID Not Found"),
        std::env::var("OSS_SECRET").expect("$OSS_SECRET Not Found"),
        std::env::var("OSS_BUCKET").expect("$OSS_BUCKET Not Found"),
        std::env::var("OSS_ENDPOINT").expect("$OSS_ENDPOINT Not Found"),
    )
}

#[test]
fn async_get_object() {
    // use your own oss config
    let oss_instance = default_oss();

    let mut rt = Runtime::new().expect("failed to start runtime");

    rt.block_on(async move {
        let buf = oss_instance
            .async_get_object("objectName", None, None)
            .await
            .unwrap();
        println!("buffer = {:?}", String::from_utf8(buf.to_vec()));
    });
}

fn get_through_sign_url(oss_instance: &OSS, sign_url: &str) -> Result<(), Error> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::DATE, oss_instance.date().parse()?);
    oss_instance.client.get(sign_url).headers(headers).send()?;
    Ok(())
}
