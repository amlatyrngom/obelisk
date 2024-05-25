use crate::bpm;

use super::MAX_KEY_SIZE;

/// Inner page structure:
/// <METADATA> <PREV_ID> <NEXT_ID> <FLAGS> <FREE_SPACE> <NUM_ENTRIES> <SLOTS> <ENTRIES>
/// The metadata is managed by the BPM. It does not belong to the BTree.
/// The flags, free_space and num_entries are 8-byte values stored at the `data_offset`
/// The slots start after the num_entries. They are 8-byte entry indexes into the page.
/// The entries start from the end and grow towards the beginning.
/// The page is full when entries cannot grow.
/// Each entry is <key_len, val_len, key, val>
pub struct BTreePage {}

impl BTreePage {
    pub fn prev_id_offset() -> usize {
        bpm::Page::data_offset()
    }

    pub fn next_id_offset() -> usize {
        Self::prev_id_offset() + 8
    }

    pub fn flags_offset() -> usize {
        Self::next_id_offset() + 8
    }

    pub fn free_space_offset() -> usize {
        Self::flags_offset() + 8
    }

    pub fn num_entries_offset() -> usize {
        Self::free_space_offset() + 8
    }

    /// Offset of the slot itself.
    pub fn slot_offset(idx: usize) -> usize {
        let offset = Self::num_entries_offset() + 8;
        let offset = offset + 8 * idx;
        offset
    }

    /// Offset of the item pointed to by the slot.
    pub fn entry_offset(page: &bpm::Page, idx: usize) -> usize {
        let offset = Self::slot_offset(idx);
        let bytes = page.read(offset, 8);
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    pub fn prev_id(page: &bpm::Page) -> i64 {
        let offset = Self::prev_id_offset();
        let bytes = page.read(offset, 8);
        i64::from_be_bytes(bytes.try_into().unwrap())
    }

    pub fn set_prev_id(page: &mut bpm::Page, prev_id: i64) {
        let offset = Self::prev_id_offset();
        let bytes = prev_id.to_be_bytes();
        page.write(offset, &bytes);
    }

    pub fn next_id(page: &bpm::Page) -> i64 {
        let offset = Self::next_id_offset();
        let bytes = page.read(offset, 8);
        i64::from_be_bytes(bytes.try_into().unwrap())
    }

    pub fn set_next_id(page: &mut bpm::Page, next_id: i64) {
        let offset = Self::next_id_offset();
        let bytes = next_id.to_be_bytes();
        page.write(offset, &bytes);
    }

    /// Return the flags.
    pub fn flags(page: &bpm::Page) -> usize {
        let offset = Self::flags_offset();
        let bytes = page.read(offset, 8);
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    /// Set the flags
    pub fn set_flags(page: &mut bpm::Page, flags: usize) {
        let offset = Self::flags_offset();
        let bytes = flags.to_be_bytes();
        page.write(offset, &bytes);
    }

    /// Return the free space in the page.
    pub fn free_space(page: &bpm::Page) -> usize {
        let offset = Self::free_space_offset();
        let bytes = page.read(offset, 8);
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    /// Set the free space.
    pub fn set_free_space(page: &mut bpm::Page, val: usize) {
        let offset = Self::free_space_offset();
        let bytes = val.to_be_bytes();
        page.write(offset, &bytes);
    }

    /// Return the number of entries in the page.
    pub fn num_entries(page: &bpm::Page) -> usize {
        let offset = Self::num_entries_offset();
        let bytes = page.read(offset, 8);
        usize::from_be_bytes(bytes.try_into().unwrap())
    }

    /// Set the number of entries.
    pub fn set_num_entries(page: &mut bpm::Page, val: usize) {
        let offset = Self::num_entries_offset();
        let bytes = val.to_be_bytes();
        page.write(offset, &bytes);
    }

    /// Return the item at the given index.
    pub fn get_item(page: &bpm::Page, idx: usize) -> (&[u8], &[u8]) {
        // Get offset.
        let offset = Self::entry_offset(page, idx);
        let mut offset = offset as usize;
        log::debug!("[BTreePage::get_item] Idx={idx}. Offset: {offset}");
        // Read key len.
        let bytes = page.read(offset - 8, 8);
        let key_len = usize::from_be_bytes(bytes.try_into().unwrap());
        offset -= 8;
        // Read val length.
        let bytes = page.read(offset - 8, 8);
        let val_len = usize::from_be_bytes(bytes.try_into().unwrap());
        offset -= 8;
        // Read key.
        let bytes = page.read(offset - key_len, key_len);
        let key = bytes;
        offset -= key_len;
        // Read val.
        let bytes = page.read(offset - val_len, val_len);
        let val = bytes;
        // Done
        (key, val)
    }

    /// Binary search to find the closest element <= key.
    /// Return the index of the element, the key, and the value.
    /// Returns None if no such element.
    pub fn find_closest_elem<'a, 'b>(
        page: &'a bpm::Page,
        key: &'b [u8],
    ) -> Option<(usize, &'a [u8], &'a [u8])> {
        let num_entries = Self::num_entries(page);
        if num_entries == 0 {
            return None;
        }
        let mut left = 0;
        let mut right = num_entries as usize - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let (mid_key, _) = Self::get_item(page, mid);
            if mid_key == key {
                left = mid;
                break;
            } else if mid_key < key {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        let (closest_key, val) = Self::get_item(page, left);
        if left == 0 && closest_key > key {
            return None;
        }
        Some((left, closest_key, val))
    }

    /// Find the last element.
    pub fn find_last_elem<'a, 'b>(page: &'a bpm::Page) -> Option<(usize, &'a [u8], &'a [u8])> {
        let num_entries = Self::num_entries(page);
        if num_entries == 0 {
            return None;
        }
        let rightmost = num_entries as usize - 1;
        let (last_key, val) = Self::get_item(page, rightmost);
        Some((rightmost, last_key, val))
    }

    /// Get item
    pub fn get<'a>(page: &'a bpm::Page, key: &[u8]) -> Option<&'a [u8]> {
        if let Some((_, found_key, val)) = Self::find_closest_elem(page, key) {
            if key == found_key {
                Some(val)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Check the page has space for one more item.
    pub fn has_space(page: &bpm::Page, key: &[u8], val: &[u8]) -> bool {
        // Get existing if any.
        let existing_val = BTreePage::get(page, key);
        if let Some(existing_val) = existing_val {
            if existing_val.len() >= val.len() {
                true
            } else {
                let diff = val.len() - existing_val.len();
                let free_space = Self::free_space(page);
                free_space >= diff
            }
        } else {
            // New entry.
            let entry_len = 8 + key.len() + 8 + val.len();
            // Total bytes written is entry_len + 8(for the slot).
            let total_len = entry_len + 8;
            let free_space: usize = Self::free_space(page);
            free_space >= total_len
        }
    }

    /// Check if too empty.
    pub fn too_empty(page: &bpm::Page, decrement_amount: usize) -> bool {
        let max_free_space = page.data.len() - Self::slot_offset(0);
        let free_space = Self::free_space(page);
        if free_space < decrement_amount {
            return true;
        }
        let free_space = free_space - decrement_amount;
        return free_space > (max_free_space / 2);
    }

    /// For inner nodes, check if the page cannot overflow due to an insert.
    pub fn is_inner_safe_for_insert(page: &bpm::Page) -> bool {
        let max_key_len = MAX_KEY_SIZE;
        let val_len = 8;
        let max_entry_len = 8 + max_key_len + 8 + val_len;
        let max_total_len = max_entry_len + 8;
        let free_space = Self::free_space(page);
        free_space >= max_total_len
    }

    /// Length of an entry (key_len, key, val_len, val).
    fn entry_len(key: &[u8], val: &[u8]) -> usize {
        8 + key.len() + 8 + val.len()
    }

    /// Total size occupied by an entry (entry + slot).
    pub fn total_len(key: &[u8], val: &[u8]) -> usize {
        Self::entry_len(key, val) + 8
    }

    /// Shift elements.
    fn shift_items(page: &mut bpm::Page, lo: usize, shift_amount: usize, advance: bool) {
        log::debug!("[BTreePage::shift_items] Lo={lo}. Amount={shift_amount}. Advance={advance}.");
        let num_entries = Self::num_entries(page);
        for idx in (lo..num_entries).rev() {
            let old_slot_offset = Self::slot_offset(idx);
            let new_slot_offset = if advance {
                old_slot_offset + 8
            } else {
                old_slot_offset
            };
            let (old_key, old_val) = Self::get_item(page, idx);
            let old_entry_len = Self::entry_len(old_key, old_val);
            let old_entry_offset_hi = Self::entry_offset(page, idx);
            let old_entry_offset_lo = old_entry_offset_hi - old_entry_len;
            let new_entry_offset = old_entry_offset_hi - shift_amount;
            log::debug!("[BTreePage::shift_items] Idx={idx}. OldSlotOffset={old_slot_offset}. NewSlotOffset={new_slot_offset}. NewEntryOffset={new_entry_offset}");
            page.write(new_slot_offset, &new_entry_offset.to_be_bytes());
            page.data.copy_within(
                old_entry_offset_lo..old_entry_offset_hi,
                new_entry_offset - old_entry_len,
            );
        }
    }

    /// Collapse elements.
    fn collapse_items(page: &mut bpm::Page, lo: usize, collapse_amount: usize, recede: bool) {
        log::debug!(
            "[BTreePage::collapse_items] Lo={lo}. Amount={collapse_amount}. Recede={recede}."
        );
        let num_entries = Self::num_entries(page);
        for idx in lo..num_entries {
            let old_slot_offset = Self::slot_offset(idx);
            let new_slot_offset = if recede {
                old_slot_offset - 8
            } else {
                old_slot_offset
            };
            let (old_key, old_val) = Self::get_item(page, idx);
            let old_entry_len = Self::entry_len(old_key, old_val);
            let old_entry_offset_hi = Self::entry_offset(page, idx);
            let old_entry_offset_lo = old_entry_offset_hi - old_entry_len;
            let new_entry_offset = old_entry_offset_hi + collapse_amount;
            log::debug!("[BTreePage::collapse_items] Idx={idx}. OldSlotOffset={old_slot_offset}. NewSlotOffset={new_slot_offset}. NewEntryOffset={new_entry_offset}");
            page.write(new_slot_offset, &new_entry_offset.to_be_bytes());
            page.data.copy_within(
                old_entry_offset_lo..old_entry_offset_hi,
                new_entry_offset - old_entry_len,
            );
        }
    }

    pub fn insert_or_append(page: &mut bpm::Page, key: &[u8], val: &[u8], force_append: bool) {
        // Must have enough space.
        debug_assert!(Self::has_space(page, key, val));
        // Get index at which to write, shifting items if necessary.
        let entry_len = Self::entry_len(key, val);
        let total_entry_len = Self::total_len(key, val);
        let curr_free_space = Self::free_space(page);
        let curr_num_entries = Self::num_entries(page);
        let prev_info = if !force_append {
            Self::find_closest_elem(page, key)
        } else {
            Self::find_last_elem(page)
        };
        let (write_idx, write_offset, free_space, num_entries) =
            if let Some((prev_idx, prev_key, prev_val)) = prev_info {
                let prev_offset = Self::entry_offset(page, prev_idx);
                let prev_entry_len = Self::entry_len(prev_key, prev_val);
                if prev_key != key {
                    // Inserting new key.
                    // Shift.
                    let write_idx = prev_idx + 1;
                    let write_offset = prev_offset - prev_entry_len;
                    Self::shift_items(page, write_idx, entry_len, true);
                    (
                        write_idx,
                        write_offset,
                        curr_free_space - total_entry_len,
                        curr_num_entries + 1,
                    )
                } else {
                    // Replacing existing key. Shift or collapse if necessary.
                    let new_free_space = if prev_entry_len < entry_len {
                        let diff = entry_len - prev_entry_len;
                        Self::shift_items(page, prev_idx, diff, false);
                        curr_free_space - diff
                    } else if prev_entry_len > entry_len {
                        let diff = prev_entry_len - entry_len;
                        Self::collapse_items(page, prev_idx + 1, prev_entry_len - entry_len, false);
                        curr_free_space + diff
                    } else {
                        curr_free_space
                    };
                    (prev_idx, prev_offset, new_free_space, curr_num_entries)
                }
            } else {
                // New item at front.
                Self::shift_items(page, 0, entry_len, true);
                (
                    0,
                    page.data.len(),
                    curr_free_space - total_entry_len,
                    curr_num_entries + 1,
                )
            };
        // Write the new slot.
        let slot_offset = Self::slot_offset(write_idx);
        page.write(slot_offset, &write_offset.to_be_bytes());
        // Write the new data.
        let mut offset = write_offset;
        // Key length
        page.write(offset - 8, &key.len().to_be_bytes());
        offset -= 8;
        // Val length.
        page.write(offset - 8, &val.len().to_be_bytes());
        offset -= 8;
        // Key.
        page.write(offset - key.len(), key);
        offset -= key.len();
        // Val.
        page.write(offset - val.len(), val);
        // Set free space and num entries.
        Self::set_free_space(page, free_space);
        Self::set_num_entries(page, num_entries);
        // Update version.
        page.update_version(page.page_version() + 1);
    }

    /// Insert the given key and value into the page.
    /// Must keep sorted order within the page.
    /// Will therefore have to shift items after insertion.
    pub fn insert(page: &mut bpm::Page, key: &[u8], val: &[u8]) {
        Self::insert_or_append(page, key, val, false)
    }

    /// Delete an item.
    pub fn delete(page: &mut bpm::Page, key: &[u8]) {
        let _ = Self::delete_with_check(page, key, false);
    }

    /// Delete with check for half full.
    /// Only fails if page would become > half-empty.
    pub fn delete_with_check(page: &mut bpm::Page, key: &[u8], check: bool) -> bool {
        let found = Self::find_closest_elem(page, key);
        if found.is_none() {
            return true;
        }
        let (idx, found_key, val) = found.unwrap();
        if found_key != key {
            return true;
        }
        // Check if would become too empty.
        let total_len = Self::total_len(key, val);
        if check && Self::too_empty(page, total_len) {
            return false;
        }
        // Collapse items.
        let entry_len = Self::entry_len(key, val);
        Self::collapse_items(page, idx + 1, entry_len, true);
        // Add entry and slot to free space.
        let free_space = Self::free_space(page) + total_len;
        Self::set_free_space(page, free_space);
        // Decrement num entries.
        let num_entries = Self::num_entries(page) - 1;
        Self::set_num_entries(page, num_entries);
        // Update version.
        page.update_version(page.page_version() + 1);
        // Done.
        true
    }

    /// Scan items between given indices.
    pub fn scan(page: &bpm::Page, lo: usize, hi: usize) -> Vec<(&[u8], &[u8])> {
        let mut items = vec![];
        for idx in lo..hi {
            let (key, val) = Self::get_item(page, idx);
            items.push((key, val));
        }
        items
    }

    /// Split page into another page.
    /// Returns the split key.
    pub fn split<'a, 'b>(
        page: &'a mut bpm::Page,
        new_page: &'b mut bpm::Page,
        needed_space: usize,
    ) -> Vec<u8> {
        Self::initialize(new_page);
        Self::initialize_is_leaf(new_page, Self::is_leaf(page));
        Self::initialize_height(new_page, Self::height(page));
        let mut num_entries = Self::num_entries(page);
        let mut split_idx = num_entries / 2;
        debug_assert!(split_idx < num_entries);
        let mut total_del_size = 0;
        for idx in split_idx..num_entries {
            let (key, val) = Self::get_item(page, idx);
            Self::insert_or_append(new_page, key, val, true);
            total_del_size += Self::total_len(key, val);
        }
        while total_del_size < needed_space {
            if split_idx == 1 {
                unimplemented!("Figure out what with keys too large. Probably just panic!");
            }
            let (key, val) = Self::get_item(page, split_idx - 1);
            Self::insert_or_append(new_page, key, val, true);
            total_del_size += Self::total_len(key, val);
            split_idx -= 1;
        }
        let (split_key, _) = Self::get_item(page, split_idx);
        let split_key = split_key.to_vec();
        // Delete items from the old page.
        // TODO: I don't think I need to explicitly delete the items. Make sure to verify this later.
        num_entries -= num_entries - split_idx;
        Self::set_num_entries(page, num_entries);
        Self::set_free_space(page, Self::free_space(page) + total_del_size);
        new_page.update_version(1);
        page.update_version(page.page_version() + 1);
        // Fix links.
        Self::set_prev_id(new_page, page.page_id());
        Self::set_next_id(new_page, Self::next_id(page));
        Self::set_next_id(page, new_page.page_id());
        split_key
    }

    /// Merge two pages.
    /// Assumes `other` is the right sibling.
    /// NOTE: Caller must fix links with next sibling.
    pub fn merge(page: &mut bpm::Page, other: &mut bpm::Page) {
        let other_num_entries = Self::num_entries(other);
        for idx in 0..other_num_entries {
            let (key, val) = Self::get_item(other, idx);
            Self::insert_or_append(page, key, val, true);
        }
        let version = page.page_version();
        page.update_version(version + 1);
    }

    /// Return the occupied space.
    pub fn occupied_space(page: &bpm::Page) -> usize {
        let initial_free_space = page.data.len() - Self::slot_offset(0);
        let free_space = Self::free_space(page);
        initial_free_space - free_space
    }

    /// Initialize a page.
    pub fn initialize(page: &mut bpm::Page) {
        // Write free space.
        let free_space = page.data.len() - Self::slot_offset(0);
        Self::set_free_space(page, free_space);
        // Write num entries.
        let num_entries = 0;
        Self::set_num_entries(page, num_entries);
    }

    /// Whether leaf or not (1 bit, offset 0).
    pub fn is_leaf(page: &bpm::Page) -> bool {
        let flags = Self::flags(page);
        (flags & 1) == 1
    }

    /// Return page height (7 bits, offset 1).
    pub fn height(page: &bpm::Page) -> usize {
        let flags = Self::flags(page);
        (flags >> 1) & 0x7F
    }

    /// Initialize leaf status.
    pub fn initialize_is_leaf(page: &mut bpm::Page, is_leaf: bool) {
        let flag = is_leaf as usize;
        let flags = Self::flags(page);
        Self::set_flags(page, flags | flag);
    }

    /// Initialize height.
    pub fn initialize_height(page: &mut bpm::Page, height: usize) {
        let flag = (height & 0x7F) << 1;
        let flags = Self::flags(page);
        Self::set_flags(page, flags | flag)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use rand::Rng;
    use rand::SeedableRng;

    use super::*;

    /// Reset test.
    fn reset_test(level: &str) {
        std::env::set_var("RUST_LOG", level);
        env_logger::builder()
            .format_timestamp(None)
            .format_level(true)
            .format_module_path(false)
            .format_target(false)
            .try_init()
            .unwrap_or(());
    }

    /// Write benchmark durations to csv file.
    fn write_durations(durations: &[(Duration, Duration, String)], filename: &str) {
        let mut writer = csv::Writer::from_path(filename).unwrap();
        for (since, d, op) in durations {
            writer
                .write_record(&[
                    since.as_secs_f64().to_string(),
                    d.as_secs_f64().to_string(),
                    op.clone(),
                ])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    #[test]
    fn basic_test() {
        let page_size = 8192;
        let key_size = 64;
        let val_size = 128;
        let mut page = bpm::Page::create(page_size, 0);
        log::debug!("Initializing");
        BTreePage::initialize(&mut page);
        let key = vec![37; key_size];
        let val = vec![73; val_size];
        log::debug!("Finding 0");
        let found = BTreePage::find_closest_elem(&page, &key);
        assert!(found.is_none());
        log::debug!("Inserting");
        BTreePage::insert(&mut page, &key, &val);
        log::debug!("Finding 1");
        let (_, found_key, found_val) = BTreePage::find_closest_elem(&page, &key).unwrap();
        assert_eq!(&key, found_key);
        assert_eq!(&val, found_val);
        assert_eq!(BTreePage::num_entries(&page), 1);

        log::debug!("Deleting");
        BTreePage::delete(&mut page, &key);
        log::debug!("Finding2");
        let found = BTreePage::find_closest_elem(&page, &key);
        assert!(found.is_none());
        assert_eq!(BTreePage::num_entries(&page), 0);
        assert_eq!(
            BTreePage::free_space(&page),
            page_size - (bpm::METADATA_SIZE + 2 * 8)
        )
    }

    fn run_advanced_test(
        insert_probability: f64,
        num_iters: usize,
        level: &str,
    ) -> Vec<(Duration, Duration, String)> {
        reset_test(level);
        let page_size = 8192;
        // Sizes to choose from. Contains a mix of odd/even/pow2 numbers.
        let sizes: Vec<usize> = vec![10, 11, 16, 100, 101, 128, 500];
        // Random contents to choose from. Should be small to allow collisions.
        let contents: Vec<u8> = vec![3, 7, 37];
        let mut expected_entries = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut page = bpm::Page::create(page_size, 0);
        log::info!("Initializing");
        BTreePage::initialize(&mut page);
        let mut expected_free_space = BTreePage::free_space(&page);
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let mut op_durations = vec![];
        let global_start_time = std::time::Instant::now();
        for i in 0..num_iters {
            log::debug!("Iterating: {i}.");
            // Check flags correctly set and non-accidentally overwritten by other operations.
            if i > 0 {
                let flags = BTreePage::flags(&page);
                assert_eq!(i - 1, flags);
            }
            BTreePage::set_flags(&mut page, i);
            // Make key.
            let key = {
                let content = contents[rng.gen::<usize>() % contents.len()];
                let size = sizes[rng.gen::<usize>() % sizes.len()];
                vec![content; size]
            };
            // Check space and num entries.
            let found_free_space = BTreePage::free_space(&page);
            log::debug!("Iterating: {i}. Expected Free Space={expected_free_space}. Found Free Space={found_free_space}");
            assert_eq!(expected_free_space, found_free_space);
            let found_num_entries = BTreePage::num_entries(&page);
            log::debug!("Iterating: {i}. Expected Entries={expected_num_entries}. Found Entries={found_num_entries}", expected_num_entries = expected_entries.len());
            assert_eq!(expected_entries.len(), found_num_entries);
            // Try getting.
            let start_time = std::time::Instant::now();
            let found = BTreePage::find_closest_elem(&page, &key);
            if let Some(expected_val) = expected_entries.get(&key) {
                op_durations.push((
                    global_start_time.elapsed(),
                    start_time.elapsed(),
                    "Get".to_string(),
                ));
                assert!(found.is_some());
                let (_, found_key, found_val) = found.unwrap();
                assert_eq!(&key, found_key);
                assert_eq!(expected_val, found_val);
            }
            // Insert with higher probability than delete.
            let should_insert = rng.gen::<f64>() <= insert_probability;
            if should_insert {
                let val = {
                    let content = contents[rng.gen::<usize>() % contents.len()];
                    let size = sizes[rng.gen::<usize>() % sizes.len()];
                    vec![content; size]
                };
                if expected_free_space < BTreePage::total_len(&key, &val) {
                    continue;
                }
                let total_len = BTreePage::total_len(&key, &val);
                expected_free_space -= total_len;
                log::debug!("Interating: {i}. Insertion. TotalLen={total_len}.");
                let start_time = std::time::Instant::now();
                BTreePage::insert(&mut page, &key, &val);
                op_durations.push((
                    global_start_time.elapsed(),
                    start_time.elapsed(),
                    "Insert".to_string(),
                ));
                if let Some(old_val) = expected_entries.insert(key.clone(), val.clone()) {
                    log::debug!("Interating: {i}. Insertion. Overwrote.");
                    expected_free_space += BTreePage::total_len(&key, &old_val);
                }
            } else {
                log::debug!("Interating: {i}. Deletion.");
                let start_time = std::time::Instant::now();
                BTreePage::delete(&mut page, &key);
                op_durations.push((
                    global_start_time.elapsed(),
                    start_time.elapsed(),
                    "Delete".to_string(),
                ));
                if let Some(old_val) = expected_entries.remove(&key) {
                    expected_free_space += BTreePage::total_len(&key, &old_val)
                } else {
                    // Remove deletes of non-existent keys for the metrics.
                    op_durations.pop();
                }
            }
        }
        op_durations
    }

    #[test]
    fn small_advanced_test() {
        run_advanced_test(0.8, 10, "debug");
    }

    #[test]
    fn advanced_test() {
        for p in [0.0, 0.1, 0.5, 0.9, 1.0] {
            run_advanced_test(p, 10000, "debug");
        }
    }

    #[test]
    fn bench_test() {
        let op_durations = run_advanced_test(0.5, 1000000, "info");
        write_durations(&op_durations, "notebooks/results/btree_page_bench.csv");
    }
}
