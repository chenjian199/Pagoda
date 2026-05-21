use std::time::Instant;

pub struct DeadlineStream<S> {
    stream: S,
    deadline: Instant,
}

impl<S> DeadlineStream<S>
where
    S: Iterator,
{
    pub fn next_item(&mut self) -> Option<S::Item> {
        if Instant::now() >= self.deadline {
            return None;
        }
        self.stream.next()
    }
}

pub fn until_deadline<S>(stream: S, deadline: Instant) -> DeadlineStream<S>
where
    S: Iterator,
{
    DeadlineStream { stream, deadline }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn stops_after_deadline() {
        let mut stream = until_deadline(0..10, Instant::now() + Duration::from_millis(20));
        let mut count = 0;
        while stream.next_item().is_some() {
            count += 1;
            thread::sleep(Duration::from_millis(5));
        }
        assert!(count > 0);
        assert!(count < 10);
    }
}