use scylla::SerializeRow;

#[derive(SerializeRow)]
#[scylla(flavor = "enforce_order")]
struct Entry {
    a: i32,
    b: Option<i32>,
    // #[scylla(flatten)]
    // inner: Inner,
}

#[derive(SerializeRow)]
#[scylla(flavor = "enforce_order")]
struct Inner {
    c: i32,
    x: bool,
}

fn main() {}
