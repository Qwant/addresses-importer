use libsqlite3_sys::ErrorCode::ConstraintViolation;

pub fn is_constraint_violation_error(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(
            libsqlite3_sys::Error {
                code: ConstraintViolation,
                extended_code: _,
            },
            _,
        ) => true,
        _ => false,
    }
}

