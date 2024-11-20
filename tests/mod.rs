use std::fs::File;
use std::io::{BufReader, Read};

mod persistence;

pub fn check_if_files_are_same(got: String, expected: String) -> bool {
    let got = File::open(got).unwrap();
    let expected = File::open(expected).unwrap();

    // Check if file sizes are different
    if got.metadata().unwrap().len() != expected.metadata().unwrap().len() {
        return false;
    }

    // Use buf readers since they are much faster
    let f1 = BufReader::new(got);
    let f2 = BufReader::new(expected);

    // Do a byte to byte comparison of the two files
    for (b1, b2) in f1.bytes().zip(f2.bytes()) {
        if b1.unwrap() != b2.unwrap() {
            return false;
        }
    }

    true
}
