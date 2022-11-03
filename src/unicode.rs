use unicode_bidi::{bidi_class, BidiClass};

pub fn str_is_ltr(s: &str) -> bool {
    s.chars().all(char_is_ltr)
}

pub fn char_is_ltr(c: char) -> bool {
    const LOWEST_RTL_CHAR: char = '\u{590}';

    if c < LOWEST_RTL_CHAR {
        true
    } else {
        class_is_ltr(bidi_class(c))
    }
}

pub fn class_is_ltr(class: BidiClass) -> bool {
    match class {
        // normal LTR stuff
        BidiClass::L
        | BidiClass::EN
        | BidiClass::ES
        | BidiClass::ET
        | BidiClass::CS
        | BidiClass::NSM
        | BidiClass::BN
        | BidiClass::B
        | BidiClass::S
        | BidiClass::WS
        | BidiClass::ON => true,
        // RTL stuff
        BidiClass::AL | BidiClass::AN | BidiClass::R => false,
        // explicit formatting
        BidiClass::LRE
        | BidiClass::LRO
        | BidiClass::RLE
        | BidiClass::RLO
        | BidiClass::PDF
        | BidiClass::LRI
        | BidiClass::RLI
        | BidiClass::FSI
        | BidiClass::PDI => false,
    }
}

#[cfg(test)]
mod test {
    use unicode_bidi::bidi_class;

    use crate::unicode::{char_is_ltr, class_is_ltr};

    #[test]
    fn char_rtl() {
        for c in '\0'..char::MAX {
            assert_eq!(char_is_ltr(c), class_is_ltr(bidi_class(c)))
        }
    }
}
