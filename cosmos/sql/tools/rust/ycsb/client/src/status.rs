//! `site.ycsb.Status`: the result of a single database operation.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Status {
    Ok,
    Error,
    NotFound,
    NotImplemented,
    UnexpectedState,
    BadRequest,
    Forbidden,
    ServiceUnavailable,
    BatchedOk,
}

impl Status {
    /// Name used in YCSB output, e.g. `[READ], Return=NOT_FOUND, 3`.
    pub fn name(self) -> &'static str {
        match self {
            Status::Ok => "OK",
            Status::Error => "ERROR",
            Status::NotFound => "NOT_FOUND",
            Status::NotImplemented => "NOT_IMPLEMENTED",
            Status::UnexpectedState => "UNEXPECTED_STATE",
            Status::BadRequest => "BAD_REQUEST",
            Status::Forbidden => "FORBIDDEN",
            Status::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            Status::BatchedOk => "BATCHED_OK",
        }
    }

    pub fn is_ok(self) -> bool {
        matches!(self, Status::Ok | Status::BatchedOk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_ok_accepts_only_ok_and_batched_ok() {
        assert!(Status::Ok.is_ok());
        assert!(Status::BatchedOk.is_ok());
        assert!(!Status::Error.is_ok());
        assert!(!Status::NotFound.is_ok());
    }

    #[test]
    fn name_matches_java_constants() {
        assert_eq!(Status::NotFound.name(), "NOT_FOUND");
        assert_eq!(Status::UnexpectedState.name(), "UNEXPECTED_STATE");
    }
}
