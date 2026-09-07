struct UTF8Iterator<'a> {
    index: usize,
    length: usize,
    string: &'a [u8],
}

impl<'a> UTF8Iterator<'a> {
    fn new(string: &[u8]) -> UTF8Iterator<'_> {
        UTF8Iterator {
            index: 0,
            length: string.len(),
            string,
        }
    }
}

impl<'a> Iterator for UTF8Iterator<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.length {
            let mut value: Self::Item = 0;
            let next = self.string[self.index] as Self::Item;
            if (next & 0x80) == 0 {
                value = next;
                self.index += 1;
            } else if (next & 0xe0) == 0xc0 {
                if (self.index + 1) < self.length {
                    let b2 = self.string[self.index + 1] as Self::Item;
                    if (b2 & 0xc0) == 0x80 {
                        value = ((value & 0x1f) << 6) | (b2 & 0x3f);
                    }
                    self.index += 2;
                }
            } else if (next & 0xf0) == 0xe0 {
                if (self.index + 2) < self.length {
                    let b2 = self.string[self.index + 1] as Self::Item;
                    let b3 = self.string[self.index + 2] as Self::Item;
                    if ((b2 & 0xc0) == 0x80) && ((b3 & 0xc0) == 0x80) {
                        value = ((value & 0x0f) << 12) | ((b2 & 0x3f) << 6) | (b3 & 0x3f);
                    }
                    self.index += 3;
                }
            } else if (next & 0xf8) == 0xf0 && (self.index + 3) < self.length {
                let b2 = self.string[self.index + 1] as Self::Item;
                let b3 = self.string[self.index + 2] as Self::Item;
                let b4 = self.string[self.index + 3] as Self::Item;
                if ((b2 & 0xc0) == 0x80) && ((b3 & 0xc0) == 0x80) && ((b4 & 0xc0) == 0x80) {
                    value = ((value & 0x07) << 18)
                        | ((b2 & 0x3f) << 12)
                        | ((b3 & 0x3f) << 6)
                        | (b4 & 0x3f);
                }
                self.index += 4;
            }
            Some(value)
        } else {
            None
        }
    }
}

pub fn qos_valid(qos: u8) -> bool {
    qos <= 2
}

pub fn client_id_valid(string: &[u8]) -> bool {
    let len = string.len();

    if len > 65535 {
        return false;
    }

    for c in UTF8Iterator::new(string) {
        if c == 0 {
            // Null not allowed
            return false;
        }
    }

    true
}

pub fn publish_topic_valid(topic: &[u8]) -> bool {
    let len = topic.len();

    if !(1..=65535).contains(&len) {
        return false;
    }

    for c in UTF8Iterator::new(topic) {
        if c == 0 {
            // Null not allowed
            return false;
        }
        if c == 0x2b {
            // + not allowed
            return false;
        }
        if c == 0x23 {
            // # not allowed
            return false;
        }
    }

    true
}

pub fn topic_filter_valid(topic: &[u8]) -> bool {
    let len = topic.len();
    let mut previous = 0;

    if !(1..=65535).contains(&len) {
        return false;
    }

    for c in UTF8Iterator::new(topic) {
        if c == 0 {
            // Null not allowed
            return false;
        }

        // #
        if previous == 0x23 {
            return false;
        }
        if c == 0x23 && previous != 0 && previous != 0x2f {
            return false;
        }

        // +
        if previous == 0x2b && c != 0x2f {
            return false;
        }
        if c == 0x2b && previous != 0 && previous != 0x2f {
            return false;
        }

        previous = c;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! to_str_vec {
        ( $x:expr ) => {
            &String::from($x).as_bytes().to_vec()
        };
    }

    #[test]
    fn test_qos_valid() {
        assert!(qos_valid(0));

        assert!(qos_valid(1));

        assert!(qos_valid(2));

        assert!(!qos_valid(3));
    }

    #[test]
    fn test_publish_topic() {
        assert!(publish_topic_valid(to_str_vec!("hello")));

        assert!(publish_topic_valid(to_str_vec!("a/b/c")));

        assert!(!publish_topic_valid(to_str_vec!("abc/+")));

        assert!(!publish_topic_valid(to_str_vec!("abc/+/def")));

        assert!(!publish_topic_valid(to_str_vec!("abc/#")));

        assert!(!publish_topic_valid(to_str_vec!("abc/#/def")));
    }

    #[test]
    fn test_filter_topic() {
        assert!(topic_filter_valid(to_str_vec!("hello")));

        assert!(topic_filter_valid(to_str_vec!("a/b/c")));

        assert!(topic_filter_valid(to_str_vec!("abc/+/def")));

        assert!(topic_filter_valid(to_str_vec!("abc/#")));

        assert!(!topic_filter_valid(to_str_vec!("abc/#/")));

        assert!(!topic_filter_valid(to_str_vec!("abc/#/def")));

        assert!(!topic_filter_valid(to_str_vec!("abc/def+/ghi")));

        assert!(!topic_filter_valid(to_str_vec!("abc/+def/ghi")));
    }
}
