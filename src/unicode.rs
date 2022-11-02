use unicode_bidi::{bidi_class, BidiClass};

pub fn str_is_ltr(s: &str) -> bool {
    s.chars().all(|c| class_is_ltr(bidi_class(c)))
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
