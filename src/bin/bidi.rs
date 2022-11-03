use std::ops::RangeInclusive;

use unicode_bidi::bidi_class;

use byte_pair_encoding::unicode::class_is_ltr;

fn main() {
    let mut rtl = 0;
    let mut total = 0;

    let mut range: Option<RangeInclusive<char>> = None;
    let mut prev = None;

    let mut rtl_ranges = vec![];

    for c in '\0'..char::MAX {
        total += 1;

        if class_is_ltr(bidi_class(c)) {
            if let Some(range) = range.clone() {
                rtl_ranges.push(range);
            }
            range = None;
        } else {
            rtl += 1;

            if let Some(prev_range) = range.clone() {
                if Some(*prev_range.end()) == prev {
                    // extend range
                    range = Some(*prev_range.start()..=c);
                } else {
                    // start new range
                    rtl_ranges.push(prev_range);
                    range = Some(c..=c);
                }
            } else {
                range = Some(c..=c);
            }

            prev = Some(c);
        }
    }

    if let Some(range) = range {
        rtl_ranges.push(range);
    }

    for range in rtl_ranges {
        println!(
            "Range {}..={} with size {}",
            range.start().escape_unicode(),
            range.end().escape_unicode(),
            range.clone().count()
        );
    }

    println!("{}/{} = {}", rtl, total, rtl as f32 / total as f32);
}
