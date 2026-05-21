use std::error::Error;

pub trait MaybeError {
    fn from_err(err: impl Error + 'static) -> Self;
    fn err(&self) -> Option<String>;

    fn is_ok(&self) -> bool {
        !self.is_err()
    }

    fn is_err(&self) -> bool {
        self.err().is_some()
    }
}