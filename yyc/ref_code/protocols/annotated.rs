use super::maybe_error::MaybeError;
use std::error::Error;

pub trait AnnotationsProvider {
    fn annotations(&self) -> Option<Vec<String>>;

    fn has_annotation(&self, annotation: &str) -> bool {
        self.annotations()
            .map(|items| items.iter().any(|item| item == annotation))
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Annotated<R> {
    pub data: Option<R>,
    pub id: Option<String>,
    pub event: Option<String>,
    pub comment: Option<Vec<String>>,
    pub error: Option<String>,
}

impl<R> Annotated<R> {
    pub fn from_data(data: R) -> Self {
        Self {
            data: Some(data),
            id: None,
            event: None,
            comment: None,
            error: None,
        }
    }

    pub fn from_error(error: String) -> Self {
        Self {
            data: None,
            id: None,
            event: Some("error".to_string()),
            comment: None,
            error: Some(error),
        }
    }

    pub fn from_annotation(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            data: None,
            id: None,
            event: Some(name.into()),
            comment: Some(vec![value.into()]),
            error: None,
        }
    }

    pub fn ok(self) -> Result<Self, String> {
        if let Some(err) = self.err() {
            return Err(err);
        }
        Ok(self)
    }

    pub fn is_event(&self) -> bool {
        self.event.is_some()
    }

    pub fn is_error(&self) -> bool {
        self.event.as_deref() == Some("error")
    }

    pub fn transfer<U>(self, data: Option<U>) -> Annotated<U> {
        Annotated {
            data,
            id: self.id,
            event: self.event,
            comment: self.comment,
            error: self.error,
        }
    }

    pub fn map_data<U, F>(self, transform: F) -> Annotated<U>
    where
        F: FnOnce(R) -> Result<U, String>,
    {
        let Annotated {
            data,
            id,
            event,
            comment,
            error,
        } = self;

        match data {
            Some(data) => match transform(data) {
                Ok(new_data) => Annotated {
                    data: Some(new_data),
                    id,
                    event,
                    comment,
                    error,
                },
                Err(err) => Annotated {
                    data: None,
                    id,
                    event: Some("error".to_string()),
                    comment,
                    error: Some(err),
                },
            },
            None => Annotated {
                data: None,
                id,
                event,
                comment,
                error,
            },
        }
    }

    pub fn into_result(self) -> Result<Option<R>, String> {
        if let Some(err) = self.err() {
            return Err(err);
        }
        Ok(self.data)
    }
}

impl<R> MaybeError for Annotated<R> {
    fn from_err(err: impl Error + 'static) -> Self {
        Self {
            data: None,
            id: None,
            event: Some("error".to_string()),
            comment: None,
            error: Some(err.to_string()),
        }
    }

    fn err(&self) -> Option<String> {
        if self.event.as_deref() != Some("error") {
            return None;
        }

        self.error
            .clone()
            .or_else(|| self.comment.as_ref().map(|items| items.join(", ")))
            .or_else(|| Some("unknown error".to_string()))
    }
}

impl<R> AnnotationsProvider for Annotated<R> {
    fn annotations(&self) -> Option<Vec<String>> {
        self.comment.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn annotated_error_roundtrip_works() {
        let annotated: Annotated<String> = Annotated::from_err(io::Error::other("boom"));
        assert!(annotated.is_err());
        assert_eq!(annotated.err().as_deref(), Some("boom"));
    }

    #[test]
    fn annotation_provider_checks_comments() {
        let annotated = Annotated::<String>::from_annotation("ttft", "12ms");
        assert!(annotated.is_event());
        assert!(annotated.has_annotation("12ms"));
    }

    #[test]
    fn from_error_populates_structured_error_field() {
        let annotated = Annotated::<String>::from_error("boom".to_string());
        assert_eq!(annotated.error.as_deref(), Some("boom"));
        assert!(annotated.comment.is_none());
    }

    #[test]
    fn map_data_transforms_or_returns_error_frame() {
        let ok = Annotated {
            data: Some(3),
            id: Some("frame-1".to_string()),
            event: Some("chunk".to_string()),
            comment: Some(vec!["trace".to_string()]),
            error: None,
        }
        .map_data(|value| Ok(value + 1));
        assert_eq!(ok.clone().into_result(), Ok(Some(4)));
        assert_eq!(ok.id.as_deref(), Some("frame-1"));
        assert_eq!(ok.event.as_deref(), Some("chunk"));

        let err: Annotated<i32> =
            Annotated::from_data(3).map_data(|_| Err("bad transform".to_string()));
        assert!(err.is_error());
        assert_eq!(err.error.as_deref(), Some("bad transform"));
    }
}