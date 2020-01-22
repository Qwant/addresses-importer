use std::convert::From;
use std::ffi::CString;
use std::ptr;

use rpostal::sys::{
    libpostal_duplicate_options_t, libpostal_duplicate_status_t, libpostal_duplicate_status_t::*,
    libpostal_is_house_number_duplicate, libpostal_is_name_duplicate,
    libpostal_is_postal_code_duplicate, libpostal_is_street_duplicate,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DuplicateStatus {
    NullDuplicate = -1,
    NonDuplicate = 0,
    PossibleDuplicate = 3,
    LikelyDuplicate = 6,
    ExactDuplicate = 9,
}

impl From<libpostal_duplicate_status_t> for DuplicateStatus {
    fn from(raw: libpostal_duplicate_status_t) -> DuplicateStatus {
        match raw {
            LIBPOSTAL_NULL_DUPLICATE_STATUS => Self::NullDuplicate,
            LIBPOSTAL_NON_DUPLICATE => Self::NonDuplicate,
            LIBPOSTAL_POSSIBLE_DUPLICATE_NEEDS_REVIEW => Self::PossibleDuplicate,
            LIBPOSTAL_LIKELY_DUPLICATE => Self::LikelyDuplicate,
            LIBPOSTAL_EXACT_DUPLICATE => Self::ExactDuplicate,
        }
    }
}

pub fn default_duplicate_option() -> libpostal_duplicate_options_t {
    libpostal_duplicate_options_t {
        num_languages: 0,
        languages: ptr::null_mut(),
    }
}

pub fn is_house_number_duplicate(field1: &str, field2: &str) -> DuplicateStatus {
    let field1 = CString::new(field1).unwrap();
    let field2 = CString::new(field2).unwrap();

    unsafe {
        libpostal_is_house_number_duplicate(
            field1.as_ptr(),
            field2.as_ptr(),
            default_duplicate_option(),
        )
        .into()
    }
}

pub fn is_name_duplicate(field1: &str, field2: &str) -> DuplicateStatus {
    let field1 = CString::new(field1).unwrap();
    let field2 = CString::new(field2).unwrap();

    unsafe {
        libpostal_is_name_duplicate(field1.as_ptr(), field2.as_ptr(), default_duplicate_option())
            .into()
    }
}

pub fn is_postal_code_duplicate(field1: &str, field2: &str) -> DuplicateStatus {
    let field1 = CString::new(field1).unwrap();
    let field2 = CString::new(field2).unwrap();

    unsafe {
        libpostal_is_postal_code_duplicate(
            field1.as_ptr(),
            field2.as_ptr(),
            default_duplicate_option(),
        )
        .into()
    }
}

pub fn is_street_duplicate(field1: &str, field2: &str) -> DuplicateStatus {
    let field1 = CString::new(field1).unwrap();
    let field2 = CString::new(field2).unwrap();

    unsafe {
        libpostal_is_street_duplicate(field1.as_ptr(), field2.as_ptr(), default_duplicate_option())
            .into()
    }
}
