/// Page.
pub struct Page {
    pub data: Vec<u8>,
}

/// Exclusive lock.
pub type ExclusivePage = guardian::ArcRwLockWriteGuardian<Page>;
/// Shared lock.
pub type SharedPage = guardian::ArcRwLockReadGuardian<Page>;

/// Metadata about a page.
#[derive(Debug, Clone)]
pub struct PageMetadata {
    pub checksum: i64,
    pub id: i64,
    pub version: i64,
}

impl Page {
    /// Create a new page.
    pub fn create(page_size: usize, page_id: i64) -> Self {
        let mut page = Self {
            data: vec![0; page_size],
        };
        let id_bytes = page_id.to_be_bytes();
        page.write(8, &id_bytes);
        page.update_version(0);
        page
    }

    /// New.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Read data from the page.
    pub fn read(&self, offset: usize, size: usize) -> &[u8] {
        &self.data[offset..offset + size]
    }

    /// Write data to the page.
    pub fn write(&mut self, offset: usize, data: &[u8]) {
        self.data[offset..offset + data.len()].copy_from_slice(data);
    }

    /// Data offset after metadata.
    pub fn data_offset() -> usize {
        super::METADATA_SIZE
    }

    /// Read the metadata of the page.
    pub fn read_metadata(&self) -> PageMetadata {
        let checksum_bytes = self.read(0, 8);
        let id_bytes = self.read(8, 8);
        let version_bytes = self.read(16, 8);
        let checksum = i64::from_be_bytes(checksum_bytes.try_into().unwrap());
        let id = i64::from_be_bytes(id_bytes.try_into().unwrap());
        let version = i64::from_be_bytes(version_bytes.try_into().unwrap());
        PageMetadata {
            checksum,
            id,
            version,
        }
    }

    /// Update the metadata of the page.
    pub fn update_version(&mut self, version: i64) {
        let version_bytes = version.to_be_bytes();
        self.write(16, &version_bytes);
    }

    pub fn rechecksum(&mut self) {
        let to_checksum = &self.data[8..];
        let checksum = crc32fast::hash(to_checksum) as i64;
        let checksum_bytes = checksum.to_be_bytes();
        self.write(0, &checksum_bytes);
    }

    /// Get the page id.
    pub fn page_id(&self) -> i64 {
        let id_bytes = self.read(8, 8);
        i64::from_be_bytes(id_bytes.try_into().unwrap())
    }

    /// Get the page version.
    pub fn page_version(&self) -> i64 {
        let version_bytes = self.read(16, 8);
        i64::from_be_bytes(version_bytes.try_into().unwrap())
    }

    /// Get the checksumn
    pub fn checksum(&self) -> i64 {
        let checksum_bytes = self.read(0, 8);
        i64::from_be_bytes(checksum_bytes.try_into().unwrap())
    }

    /// Check if the page was fully written.
    pub fn is_complete(&self) -> bool {
        let checksum_bytes = self.read(0, 8);
        let expected_checksum = i64::from_be_bytes(checksum_bytes.try_into().unwrap());
        let to_checksum = &self.data[8..];
        let actual_checksum = crc32fast::hash(to_checksum) as i64;
        expected_checksum == actual_checksum
    }
}
