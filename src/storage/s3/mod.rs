pub mod client_builder;

use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::RequestPayer;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use tracing::debug;

use crate::config::ClientConfig;
use crate::storage::StorageTrait;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ListEntry, S3Object, VersionInfo};
use leaky_bucket::RateLimiter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

const EXPRESS_ONEZONE_STORAGE_SUFFIX: &str = "--x-s3";

/// Whether to use ListObjectsV2 or ListObjectVersions.
#[derive(Clone, Copy)]
pub(crate) enum ListingMode {
    Objects,
    Versions,
}

/// A single page of listing results from S3.
pub(crate) struct ListPage {
    pub objects: Vec<ListEntry>,
    pub sub_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub continuation_token: Option<String>,
    pub key_marker: Option<String>,
    pub version_id_marker: Option<String>,
}

/// Trait abstracting the page-fetching call so the listing algorithm can be tested
/// without a real S3 client.
///
/// `continuation_token` and `start_after` apply to ListObjectsV2 (`start_after`
/// only positions a request that carries no continuation token);
/// `key_marker` / `version_id_marker` apply to ListObjectVersions.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub(crate) trait PageFetcher: Send + Sync {
    async fn fetch_page(
        &self,
        mode: ListingMode,
        max_keys: i32,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        start_after: Option<&str>,
    ) -> Result<ListPage>;
}

// ---------------------------------------------------------------------------
// S3PageFetcher — real AWS SDK implementation of PageFetcher
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct S3PageFetcher {
    client: Client,
    bucket: String,
    request_payer: Option<RequestPayer>,
    fetch_owner: bool,
    fetch_restore_status: bool,
}

impl S3PageFetcher {
    async fn fetch_list_objects_page(
        &self,
        max_keys: i32,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
    ) -> Result<ListPage> {
        tracing::trace!(
            bucket = %self.bucket,
            prefix = ?prefix,
            delimiter = ?delimiter,
            max_keys,
            continuation_token = ?continuation_token,
            start_after = ?start_after,
            "ListObjectsV2 request"
        );
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .max_keys(max_keys);

        if let Some(prefix) = prefix {
            req = req.prefix(prefix);
        }
        if let Some(delimiter) = delimiter {
            req = req.delimiter(delimiter);
        }
        if let Some(token) = continuation_token {
            req = req.continuation_token(token);
        }
        if let Some(start_after) = start_after {
            req = req.start_after(start_after);
        }
        if let Some(ref payer) = self.request_payer {
            req = req.request_payer(payer.clone());
        }
        if self.fetch_owner {
            req = req.fetch_owner(true);
        }
        if self.fetch_restore_status {
            req = req.optional_object_attributes(
                aws_sdk_s3::types::OptionalObjectAttributes::RestoreStatus,
            );
        }

        let response = req.send().await.map_err(|e| {
            let (code, msg) = extract_sdk_error_details(&e);
            tracing::error!(
                bucket = %self.bucket,
                prefix = ?prefix,
                s3_error_code = %code,
                s3_error_message = %msg,
                "S3 ListObjectsV2 API call failed"
            );
            anyhow::anyhow!(e).context(format!(
                "S3 ListObjectsV2 failed for s3://{}/{}",
                self.bucket,
                prefix.unwrap_or("")
            ))
        })?;

        let objects: Vec<ListEntry> = response
            .contents()
            .iter()
            .filter_map(convert_object)
            .collect();

        let sub_prefixes: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix().map(|p| p.to_string()))
            .collect();

        Ok(ListPage {
            objects,
            sub_prefixes,
            is_truncated: response.is_truncated() == Some(true),
            continuation_token: response.next_continuation_token().map(|s| s.to_string()),
            key_marker: None,
            version_id_marker: None,
        })
    }

    async fn fetch_list_versions_page(
        &self,
        max_keys: i32,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> Result<ListPage> {
        tracing::trace!(
            bucket = %self.bucket,
            prefix = ?prefix,
            delimiter = ?delimiter,
            max_keys,
            key_marker = ?key_marker,
            version_id_marker = ?version_id_marker,
            "ListObjectVersions request"
        );
        let mut req = self
            .client
            .list_object_versions()
            .bucket(&self.bucket)
            .max_keys(max_keys);

        if let Some(prefix) = prefix {
            req = req.prefix(prefix);
        }
        if let Some(delimiter) = delimiter {
            req = req.delimiter(delimiter);
        }
        if let Some(marker) = key_marker {
            req = req.key_marker(marker);
        }
        if let Some(marker) = version_id_marker {
            req = req.version_id_marker(marker);
        }
        if let Some(ref payer) = self.request_payer {
            req = req.request_payer(payer.clone());
        }
        // ListObjectVersions always returns Owner, so no fetch_owner toggle.
        // RestoreStatus, however, must be opted into via OptionalObjectAttributes.
        if self.fetch_restore_status {
            req = req.optional_object_attributes(
                aws_sdk_s3::types::OptionalObjectAttributes::RestoreStatus,
            );
        }

        let response = req.send().await.map_err(|e| {
            let (code, msg) = extract_sdk_error_details(&e);
            tracing::error!(
                bucket = %self.bucket,
                prefix = ?prefix,
                s3_error_code = %code,
                s3_error_message = %msg,
                "S3 ListObjectVersions API call failed"
            );
            anyhow::anyhow!(e).context(format!(
                "S3 ListObjectVersions failed for s3://{}/{}",
                self.bucket,
                prefix.unwrap_or("")
            ))
        })?;

        let mut objects: Vec<ListEntry> = Vec::new();

        for version in response.versions() {
            if let Some(entry) = convert_object_version(version) {
                objects.push(entry);
            }
        }

        for marker in response.delete_markers() {
            if let Some(entry) = convert_delete_marker(marker) {
                objects.push(entry);
            }
        }

        let sub_prefixes: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix().map(|p| p.to_string()))
            .collect();

        Ok(ListPage {
            objects,
            sub_prefixes,
            is_truncated: response.is_truncated() == Some(true),
            continuation_token: None,
            key_marker: response.next_key_marker().map(|s| s.to_string()),
            version_id_marker: response.next_version_id_marker().map(|s| s.to_string()),
        })
    }
}

#[async_trait]
impl PageFetcher for S3PageFetcher {
    async fn fetch_page(
        &self,
        mode: ListingMode,
        max_keys: i32,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        start_after: Option<&str>,
    ) -> Result<ListPage> {
        match mode {
            ListingMode::Objects => {
                self.fetch_list_objects_page(
                    max_keys,
                    prefix,
                    delimiter,
                    continuation_token,
                    start_after,
                )
                .await
            }
            ListingMode::Versions => {
                self.fetch_list_versions_page(
                    max_keys,
                    prefix,
                    delimiter,
                    key_marker,
                    version_id_marker,
                )
                .await
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Key-range splitting
// ---------------------------------------------------------------------------

/// Upper bound on how many times one prefix may be re-split into key ranges
/// (each split nests one level deeper). Every split first pages through at
/// least `parallel_range_split_threshold` keys, so recursion always makes
/// progress; this cap is only a defensive backstop.
const MAX_RANGE_SPLIT_DEPTH: u16 = 32;

/// Maximum number of key positions probed when locating where the keys of a
/// range stop sharing a common prefix.
const MAX_PROBE_POSITIONS: usize = 128;

/// Maximum number of boundary probes fired per split. Larger observed
/// alphabets are sampled evenly down to this many candidates.
const MAX_BOUNDARY_PROBES: usize = 64;

/// The portion of a key space one listing task is responsible for.
///
/// Bounds are exclusive at the start and inclusive at the end so that a key
/// equal to a boundary belongs to exactly one range: `start-after` (and
/// `key-marker`) skip the boundary key itself, and the range below stops only
/// once it sees a key strictly greater than its `end_inclusive`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct KeyRange {
    /// Start listing strictly after this key (`start-after` for
    /// ListObjectsV2, `key-marker` for ListObjectVersions).
    start_after: Option<String>,
    /// Version-id marker paired with `start_after` (ListObjectVersions only).
    /// When set, the listing resumes in the middle of `start_after`'s
    /// versions, so entries whose key equals `start_after` are expected.
    version_id_marker: Option<String>,
    /// Stop once an entry's key exceeds this key.
    end_inclusive: Option<String>,
}

impl KeyRange {
    /// True if an endpoint honouring `start-after` could never have returned
    /// `key` for this range.
    fn is_before_start(&self, key: &str) -> bool {
        match &self.start_after {
            None => false,
            Some(start) if self.version_id_marker.is_some() => key < start.as_str(),
            Some(start) => key <= start.as_str(),
        }
    }

    fn is_beyond_end(&self, key: &str) -> bool {
        self.end_inclusive.as_deref().is_some_and(|end| key > end)
    }
}

/// One unit of work for the parallel listing engine: a prefix, the key range
/// within it, and where the task sits in the prefix tree and split chain.
#[derive(Clone)]
struct ListTask {
    prefix: Option<String>,
    /// Depth in the prefix tree (0 = the engine's own prefix).
    depth: u16,
    range: KeyRange,
    /// How many range splits produced this task (0 = whole prefix).
    split_depth: u16,
    /// Synthetic `--max-depth` boundary prefixes already emitted for this
    /// prefix. Shared by every range of the same prefix so a boundary
    /// prefix whose keys straddle two ranges is emitted exactly once.
    emitted_prefixes: Arc<Mutex<HashSet<String>>>,
}

/// Smallest Unicode scalar value greater than `c`, skipping the surrogate gap.
fn next_char(c: char) -> Option<char> {
    let code = c as u32 + 1;
    let code = if (0xD800..=0xDFFF).contains(&code) {
        0xE000
    } else {
        code
    };
    char::from_u32(code)
}

/// Longest common prefix, in chars, of all `keys`.
fn common_prefix_chars(keys: &[&str]) -> Vec<char> {
    let mut lcp: Vec<char> = match keys.first() {
        Some(first) => first.chars().collect(),
        None => return Vec::new(),
    };
    for key in &keys[1..] {
        let shared = lcp
            .iter()
            .zip(key.chars())
            .take_while(|(a, b)| **a == *b)
            .count();
        lcp.truncate(shared);
        if lcp.is_empty() {
            break;
        }
    }
    lcp
}

/// Characters to probe for range boundaries after `after`: the characters
/// the page itself uses (`alphabet`, everything seen at or beyond the branch
/// position), plus the code point right after `after` so that at least one
/// probe finds the next branch even when the page's alphabet is exhausted.
/// Digits, hex, base64, a handful of CJK characters: whatever the keys are
/// made of, the page shows it, and no probe is spent on characters the keys
/// never use. Alphabets larger than `MAX_BOUNDARY_PROBES` are sampled evenly.
///
/// The probe set is only a balance heuristic: ranges are contiguous and the
/// last one is open-ended, so an unrepresentative page costs an extra split
/// later, never a key.
fn candidate_boundary_chars(
    after: Option<char>,
    alphabet: &BTreeSet<char>,
    delimiter_mode: bool,
) -> Vec<char> {
    let mut chars: Vec<char> = alphabet
        .iter()
        .copied()
        .chain(after.and_then(next_char))
        .filter(|c| after.is_none_or(|a| *c > a))
        // With a "/" delimiter, a boundary ending in "/" could fall inside a
        // common prefix and make two ranges report it.
        .filter(|c| !(delimiter_mode && *c == '/'))
        .collect();
    chars.sort_unstable();
    chars.dedup();
    if chars.len() > MAX_BOUNDARY_PROBES {
        let step = chars.len() as f64 / MAX_BOUNDARY_PROBES as f64;
        chars = (0..MAX_BOUNDARY_PROBES)
            .map(|i| chars[(i as f64 * step) as usize])
            .collect();
    }
    chars
}

// ---------------------------------------------------------------------------
// ListingEngine — the listing algorithm, parameterized over any PageFetcher
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct ListingEngine<F: PageFetcher + Clone> {
    fetcher: F,
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    cancellation_token: PipelineCancellationToken,
    max_parallel_listings: u16,
    max_parallel_listing_max_depth: u16,
    allow_parallel_listings_in_express_one_zone: bool,
    listing_worker_semaphore: Arc<tokio::sync::Semaphore>,
    max_depth: Option<u16>,
    rate_limiter: Option<Arc<RateLimiter>>,
    api_call_counter: Arc<AtomicU64>,
    /// Keys a task pages through sequentially before its remainder is split
    /// into key ranges. 0 disables range splitting.
    parallel_range_split_threshold: u32,
    /// Set once the endpoint is seen returning keys at or before
    /// `start-after`; range splitting is disabled from then on.
    range_split_unsupported: Arc<AtomicBool>,
    /// Number of range splits performed (diagnostics and tests).
    range_split_counter: Arc<AtomicU64>,
}

impl<F: PageFetcher + Clone + 'static> ListingEngine<F> {
    /// Returns true if the bucket is an Express One Zone bucket.
    fn is_express_onezone_storage(&self) -> bool {
        self.bucket.ends_with(EXPRESS_ONEZONE_STORAGE_SUFFIX)
    }

    /// Acquire a rate limiter token before making an S3 API call.
    /// Returns `true` if cancelled while waiting.
    async fn acquire_rate_limit(&self) -> bool {
        if let Some(ref rate_limiter) = self.rate_limiter {
            tokio::select! {
                _ = rate_limiter.acquire_one() => false,
                _ = self.cancellation_token.cancelled() => true,
            }
        } else {
            false
        }
    }

    /// Send a batch of entries to the channel, returning `true` if sending
    /// should stop (cancellation or receiver dropped).
    /// When `max_depth` is set, entries beyond the depth limit are replaced
    /// by synthetic CommonPrefix entries at the boundary depth.
    async fn send_listed_entries(
        &self,
        entries: Vec<ListEntry>,
        sender: &Sender<ListEntry>,
        emitted_prefixes: Option<&Mutex<HashSet<String>>>,
    ) -> Result<bool> {
        for entry in entries {
            if self.cancellation_token.is_cancelled() {
                return Ok(true);
            }
            if let Some(max_depth) = self.max_depth
                && let Some(depth) = self.key_depth(entry.key())
                && depth > max_depth
            {
                // Synthesize a CommonPrefix at the boundary depth
                if let (Some(prefix_at_boundary), Some(seen)) = (
                    self.prefix_at_depth(entry.key(), max_depth),
                    emitted_prefixes,
                ) {
                    let is_new = seen
                        .lock()
                        .expect("emitted-prefix set poisoned")
                        .insert(prefix_at_boundary.clone());
                    if is_new
                        && sender
                            .send(ListEntry::CommonPrefix(prefix_at_boundary))
                            .await
                            .is_err()
                    {
                        return Ok(true);
                    }
                }
                continue;
            }
            if sender.send(entry).await.is_err() {
                return Ok(true); // receiver dropped
            }
        }
        Ok(false)
    }

    /// Calculate the depth of a key relative to the engine's prefix.
    /// Depth = (number of "/" in the relative part of the key) + 1.
    /// Returns None if the key doesn't start with the prefix.
    ///
    /// A trailing "/" on the key (directory-marker object) is stripped
    /// before counting, so "foo/bar/" under prefix "foo/" has the same
    /// depth (1) as "foo/bar.txt" — they're at the same hierarchical
    /// level.
    fn key_depth(&self, key: &str) -> Option<u16> {
        let prefix = self.prefix.as_deref().unwrap_or("");
        let relative = key.strip_prefix(prefix)?;
        if relative.is_empty() {
            return None; // key IS the prefix, not an object under it
        }
        // Strip a single trailing "/" so directory-marker objects are
        // counted at the same depth as regular objects in the same "folder".
        let counted = relative.strip_suffix('/').unwrap_or(relative);
        let slash_count = counted.matches('/').count();
        // "file.txt"      => 0 slashes => depth 1
        // "sub/"          => 0 slashes => depth 1 (directory marker, same level)
        // "a/file.txt"    => 1 slash  => depth 2
        Some(slash_count as u16 + 1)
    }

    /// Extract the prefix at a given depth boundary from a key.
    /// For key "p/a/b/file.txt" with engine prefix "p/" and max_depth=1,
    /// the boundary prefix is "p/a/" (the first directory component beyond
    /// the depth limit, ending with "/").
    fn prefix_at_depth(&self, key: &str, max_depth: u16) -> Option<String> {
        let prefix = self.prefix.as_deref().unwrap_or("");
        let relative = key.strip_prefix(prefix)?;
        // Find the (max_depth)th "/" in the relative part
        let mut slash_count = 0u16;
        for (i, ch) in relative.char_indices() {
            if ch == '/' {
                slash_count += 1;
                if slash_count == max_depth {
                    return Some(format!("{}{}", prefix, &relative[..=i]));
                }
            }
        }
        None
    }

    /// Decide whether to use parallel or sequential listing, then dispatch.
    pub(crate) async fn list_dispatch(
        &self,
        mode: ListingMode,
        sender: &Sender<ListEntry>,
        max_keys: i32,
    ) -> Result<()> {
        let use_parallel = self.max_parallel_listings > 1
            && self.delimiter.is_none() // recursive mode
            && (!self.is_express_onezone_storage()
                || self.allow_parallel_listings_in_express_one_zone);

        if use_parallel {
            debug!(
                bucket = %self.bucket,
                max_parallel = self.max_parallel_listings,
                max_depth = self.max_parallel_listing_max_depth,
                range_split_threshold = self.parallel_range_split_threshold,
                "Using parallel listing"
            );
            let permit = self
                .listing_worker_semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("listing semaphore closed unexpectedly");
            let task = ListTask {
                prefix: self.prefix.clone(),
                depth: 0,
                range: KeyRange::default(),
                split_depth: 0,
                emitted_prefixes: Arc::new(Mutex::new(HashSet::new())),
            };
            self.list_with_parallel(mode, sender, max_keys, task, permit)
                .await
        } else {
            debug!(bucket = %self.bucket, "Using sequential listing");
            self.list_sequential(
                mode,
                sender,
                max_keys,
                self.prefix.clone(),
                self.delimiter.clone(),
            )
            .await
        }
    }

    /// Sequential listing: paginate through all results for the given prefix/delimiter.
    async fn list_sequential(
        &self,
        mode: ListingMode,
        sender: &Sender<ListEntry>,
        max_keys: i32,
        prefix: Option<String>,
        delimiter: Option<String>,
    ) -> Result<()> {
        let mut continuation_token: Option<String> = None;
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;
        let emitted_prefixes = Mutex::new(HashSet::new());

        loop {
            if self.cancellation_token.is_cancelled() {
                debug!("list_sequential cancelled");
                break;
            }

            if self.acquire_rate_limit().await {
                debug!("list_sequential rate-limit wait cancelled");
                break;
            }
            self.api_call_counter.fetch_add(1, Ordering::Relaxed);
            let page = self
                .fetcher
                .fetch_page(
                    mode,
                    max_keys,
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    continuation_token.as_deref(),
                    key_marker.as_deref(),
                    version_id_marker.as_deref(),
                    None,
                )
                .await?;

            // Send objects (with synthetic CommonPrefix for keys beyond max_depth)
            if self
                .send_listed_entries(page.objects, sender, Some(&emitted_prefixes))
                .await?
            {
                return Ok(());
            }

            // Send common prefixes
            let prefix_entries: Vec<ListEntry> = page
                .sub_prefixes
                .iter()
                .map(|p| ListEntry::CommonPrefix(p.clone()))
                .collect();
            if self
                .send_listed_entries(prefix_entries, sender, Some(&emitted_prefixes))
                .await?
            {
                return Ok(());
            }

            // Check for more pages
            if page.is_truncated {
                match mode {
                    ListingMode::Objects => {
                        let next_token = page.continuation_token;
                        if next_token.is_none() {
                            anyhow::bail!(
                                "S3 returned truncated response but no continuation token for s3://{}/{}",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                        // A buggy S3-compatible endpoint that returns the same
                        // token forever would otherwise loop indefinitely.
                        if next_token == continuation_token {
                            anyhow::bail!(
                                "S3 returned the same continuation token twice for s3://{}/{}; \
                                 refusing to loop. This is likely a bug in the S3-compatible endpoint.",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                        continuation_token = next_token;
                    }
                    ListingMode::Versions => {
                        let next_key_marker = page.key_marker;
                        let next_version_id_marker = page.version_id_marker;
                        if next_key_marker.is_none() {
                            anyhow::bail!(
                                "S3 returned truncated response but no next key marker for s3://{}/{}",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                        if next_key_marker == key_marker
                            && next_version_id_marker == version_id_marker
                        {
                            anyhow::bail!(
                                "S3 returned the same key/version marker twice for s3://{}/{}; \
                                 refusing to loop. This is likely a bug in the S3-compatible endpoint.",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                        key_marker = next_key_marker;
                        version_id_marker = next_version_id_marker;
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Drop the entries of a page that fall outside `range`.
    ///
    /// Returns the clipped objects and sub-prefixes plus whether the page
    /// crossed the range's upper bound (no further pages are needed).
    /// Objects at or before the range start can only come from an endpoint
    /// that ignores `start-after`; they are dropped and range splitting is
    /// disabled for the rest of the run so the listing stays correct.
    fn clip_page_to_range(
        &self,
        range: &KeyRange,
        mut objects: Vec<ListEntry>,
        mut sub_prefixes: Vec<String>,
    ) -> (Vec<ListEntry>, Vec<String>, bool) {
        let mut reached_end = false;
        let mut start_after_ignored = false;
        objects.retain(|entry| {
            let key = entry.key();
            if range.is_before_start(key) {
                start_after_ignored = true;
                false
            } else if range.is_beyond_end(key) {
                reached_end = true;
                false
            } else {
                true
            }
        });
        // A common prefix never straddles a range boundary (boundaries are
        // placed at a character position above every prefix at this level),
        // so comparing the prefix string itself is sufficient.
        sub_prefixes.retain(|cp| {
            if range.is_beyond_end(cp) {
                reached_end = true;
                false
            } else {
                true
            }
        });
        if start_after_ignored && !self.range_split_unsupported.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                bucket = %self.bucket,
                "Endpoint returned keys at or before start-after; disabling key-range splitting"
            );
        }
        (objects, sub_prefixes, reached_end)
    }

    /// Whether a task that has listed `listed_keys` keys should split the
    /// rest of its range instead of continuing sequentially. `next_split_at`
    /// is the key count at which the task may next try (initially the
    /// threshold; a threshold later after an attempt found no boundary, so a
    /// page that reveals new characters gets another chance without probing
    /// on every page).
    ///
    /// Splitting only pays off when workers are idle: with every permit in
    /// use, more tasks would just queue behind the semaphore while the probe
    /// requests and per-range boundary pages add cost. `available_permits`
    /// is therefore the trigger, which naturally covers both a flat bucket
    /// (one task, many idle workers) and the tail of a skewed tree (one
    /// huge leaf left after the small ones finish).
    fn should_split_range(&self, task: &ListTask, listed_keys: u64, next_split_at: u64) -> bool {
        self.parallel_range_split_threshold > 0
            && task.split_depth < MAX_RANGE_SPLIT_DEPTH
            && listed_keys >= next_split_at
            // Directory buckets do not support start-after.
            && !self.is_express_onezone_storage()
            && !self.range_split_unsupported.load(Ordering::Relaxed)
            && self.listing_worker_semaphore.available_permits() > 0
    }

    /// Smallest key strictly after `after` under `prefix`, or `None`.
    /// One `max-keys=1` request; counts against the semaphore, the rate
    /// limiter and the API call counter like any other listing request.
    async fn probe_next_key(
        &self,
        mode: ListingMode,
        prefix: Option<&str>,
        after: &str,
    ) -> Result<Option<String>> {
        if self.cancellation_token.is_cancelled() {
            return Ok(None);
        }
        let _permit = self
            .listing_worker_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("listing semaphore closed unexpectedly");
        if self.acquire_rate_limit().await {
            return Ok(None);
        }
        self.api_call_counter.fetch_add(1, Ordering::Relaxed);
        let page = match mode {
            ListingMode::Objects => {
                self.fetcher
                    .fetch_page(mode, 1, prefix, None, None, None, None, Some(after))
                    .await?
            }
            ListingMode::Versions => {
                self.fetcher
                    .fetch_page(mode, 1, prefix, None, None, Some(after), None, None)
                    .await?
            }
        };
        Ok(page
            .objects
            .iter()
            .map(|entry| entry.key().to_string())
            .min())
    }

    /// Run [`Self::probe_next_key`] for every target concurrently. The result
    /// vector is aligned with `targets`.
    async fn probe_next_keys(
        &self,
        mode: ListingMode,
        prefix: Option<&str>,
        targets: Vec<String>,
    ) -> Result<Vec<Option<String>>> {
        let mut results = vec![None; targets.len()];
        let mut join_set = JoinSet::new();
        for (index, target) in targets.into_iter().enumerate() {
            let engine = self.clone();
            let prefix = prefix.map(str::to_string);
            join_set.spawn(async move {
                engine
                    .probe_next_key(mode, prefix.as_deref(), &target)
                    .await
                    .map(|found| (index, found))
            });
        }
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((index, found))) => results[index] = found,
                Ok(Err(e)) => {
                    self.cancellation_token.cancel();
                    return Err(e);
                }
                Err(join_err) => {
                    self.cancellation_token.cancel();
                    return Err(anyhow::anyhow!("Range probe task panicked: {}", join_err));
                }
            }
        }
        Ok(results)
    }

    /// Split the remainder of a range, `(cursor, end]`, into sub-ranges.
    ///
    /// A page only reveals the smallest keys of a range, so the distribution
    /// beyond `cursor` is observed with `max-keys=1` probes (`start-after=X`
    /// answers "what is the first key after X?"):
    ///
    /// 1. Take the longest common prefix `lcp` of the page's keys. For every
    ///    position `q` in it, probe just past `lcp[..q]`; the smallest `q`
    ///    whose answer still shares `lcp[..q]` is where the remaining keys
    ///    really branch (`q*`). This skips shared runs like `file00` in one
    ///    round instead of peeling them one character at a time.
    /// 2. Probe `base + c` for every character `c` the page uses beyond that
    ///    position that sorts after the character observed at `q*` (see
    ///    [`candidate_boundary_chars`]). Each answer's character at `q*` is a
    ///    real branch; the distinct ones become the boundaries.
    ///
    /// Ranges are contiguous and the last one keeps the parent's upper
    /// bound, so keys with unexpected characters (any script, any byte) land
    /// in some range regardless of how the probes were placed; a poor
    /// placement only leaves a big range that splits again later.
    ///
    /// Returns `None` when no boundary was found, in which case the caller
    /// keeps listing sequentially.
    async fn plan_range_split(
        &self,
        mode: ListingMode,
        prefix: Option<&str>,
        delimiter_mode: bool,
        cursor: KeyRange,
        page_keys: &[String],
    ) -> Result<Option<Vec<KeyRange>>> {
        let p = prefix.unwrap_or("");
        let cursor_key = cursor
            .start_after
            .clone()
            .expect("range split cursor must carry a key");
        let end = cursor.end_inclusive.clone();
        let within_end = |key: &str| end.as_deref().is_none_or(|e| key <= e);

        let relative_keys: Vec<&str> = page_keys.iter().filter_map(|k| k.strip_prefix(p)).collect();
        let lcp = common_prefix_chars(&relative_keys);
        let base_at = |q: usize| -> String {
            let mut s = String::with_capacity(p.len() + q * 4);
            s.push_str(p);
            s.extend(lcp[..q].iter());
            s
        };

        // Round 1: where do the remaining keys branch off the page's lcp?
        let mut positions: Vec<usize> = Vec::new();
        let mut targets: Vec<String> = Vec::new();
        for (q, &ch) in lcp.iter().enumerate().take(MAX_PROBE_POSITIONS) {
            if let Some(next) = next_char(ch) {
                let mut target = base_at(q);
                target.push(next);
                if within_end(&target) {
                    positions.push(q);
                    targets.push(target);
                }
            }
        }
        let mut q_star = lcp.len();
        for (q, found) in positions
            .iter()
            .zip(self.probe_next_keys(mode, prefix, targets).await?)
        {
            if let Some(key) = found
                && within_end(&key)
                && key.starts_with(&base_at(*q))
            {
                q_star = q_star.min(*q);
            }
        }
        let base = base_at(q_star);
        let observed_char = if q_star < lcp.len() {
            Some(lcp[q_star])
        } else {
            cursor_key
                .strip_prefix(base.as_str())
                .and_then(|rest| rest.chars().next())
        };

        // Round 2: which characters actually occur at q* after the cursor?
        let alphabet: BTreeSet<char> = relative_keys
            .iter()
            .flat_map(|key| key.chars().skip(q_star))
            .collect();
        let targets: Vec<String> =
            candidate_boundary_chars(observed_char, &alphabet, delimiter_mode)
                .into_iter()
                .map(|c| {
                    let mut target = base.clone();
                    target.push(c);
                    target
                })
                .filter(|target| within_end(target))
                .collect();
        let mut boundaries: Vec<String> = self
            .probe_next_keys(mode, prefix, targets)
            .await?
            .into_iter()
            .flatten()
            .filter(|key| within_end(key))
            .filter_map(|key| {
                let ch = key.strip_prefix(base.as_str())?.chars().next()?;
                (!(delimiter_mode && ch == '/')).then(|| {
                    let mut boundary = base.clone();
                    boundary.push(ch);
                    boundary
                })
            })
            // Always true for a compliant endpoint; guards against one that
            // ignores start-after and answers with the first key overall.
            .filter(|boundary| boundary.as_str() > cursor_key.as_str())
            .collect();
        boundaries.sort_unstable();
        boundaries.dedup();

        if boundaries.is_empty() {
            debug!(
                prefix = %p,
                cursor = %cursor_key,
                "No key-range boundaries found; continuing sequentially"
            );
            return Ok(None);
        }

        let mut ranges = Vec::with_capacity(boundaries.len() + 1);
        let mut start_after = cursor.start_after;
        let mut version_id_marker = cursor.version_id_marker;
        for boundary in boundaries {
            ranges.push(KeyRange {
                start_after: start_after.take(),
                version_id_marker: version_id_marker.take(),
                end_inclusive: Some(boundary.clone()),
            });
            start_after = Some(boundary);
        }
        ranges.push(KeyRange {
            start_after,
            version_id_marker: None,
            end_inclusive: end,
        });

        self.range_split_counter.fetch_add(1, Ordering::Relaxed);
        debug!(
            prefix = %p,
            cursor = %cursor_key,
            branch_position = q_star,
            ranges = ranges.len(),
            "Splitting key range"
        );
        Ok(Some(ranges))
    }

    /// Parallel listing: one task per (prefix, key range), recursive.
    ///
    /// Tasks at depths up to `max_parallel_listing_max_depth` list with a
    /// "/" delimiter and spawn a child per discovered sub-prefix (prefix
    /// discovery); deeper tasks list without a delimiter. Either kind, after
    /// paging through `parallel_range_split_threshold` keys while workers are
    /// idle, hands the rest of its range to concurrently listed sub-ranges
    /// (see [`Self::plan_range_split`]). This keeps flat key spaces and
    /// lopsided trees parallel even though they expose no sub-prefixes.
    fn list_with_parallel<'a>(
        &'a self,
        mode: ListingMode,
        sender: &'a Sender<ListEntry>,
        max_keys: i32,
        task: ListTask,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if self.cancellation_token.is_cancelled() {
                return Ok(());
            }

            // Content depth limit: at recursion depth D, objects have key-depth D+1.
            // Stop fetching when key-depth would exceed max_depth.
            if let Some(max_depth) = self.max_depth
                && task.depth >= max_depth
            {
                return Ok(());
            }

            let discovery = task.depth <= self.max_parallel_listing_max_depth;
            let delimiter = if discovery { Some("/") } else { None };
            let prefix = task.prefix.as_deref();

            // Hold `permit` across the whole pagination of this range so it
            // counts against `max_parallel_listings`. It is released before
            // probing or spawning children, and every child (probe, range,
            // sub-prefix) acquires its own permit, so this cannot deadlock:
            // nothing waits for a permit while holding one.
            let mut permit = Some(permit);

            let mut continuation_token: Option<String> = None;
            let mut key_marker: Option<String> = task.range.start_after.clone();
            let mut version_id_marker: Option<String> = task.range.version_id_marker.clone();
            let mut all_sub_prefixes: Vec<String> = Vec::new();
            let mut listed_keys: u64 = 0;
            let mut next_split_at = u64::from(self.parallel_range_split_threshold);
            let mut range_children: Vec<KeyRange> = Vec::new();

            loop {
                if self.cancellation_token.is_cancelled() {
                    return Ok(());
                }

                if self.acquire_rate_limit().await {
                    return Ok(());
                }
                self.api_call_counter.fetch_add(1, Ordering::Relaxed);
                let page = match mode {
                    ListingMode::Objects => {
                        // start-after positions only the first request; later
                        // pages resume from the continuation token.
                        let start_after = if continuation_token.is_none() {
                            task.range.start_after.as_deref()
                        } else {
                            None
                        };
                        self.fetcher
                            .fetch_page(
                                mode,
                                max_keys,
                                prefix,
                                delimiter,
                                continuation_token.as_deref(),
                                None,
                                None,
                                start_after,
                            )
                            .await?
                    }
                    ListingMode::Versions => {
                        self.fetcher
                            .fetch_page(
                                mode,
                                max_keys,
                                prefix,
                                delimiter,
                                None,
                                key_marker.as_deref(),
                                version_id_marker.as_deref(),
                                None,
                            )
                            .await?
                    }
                };
                let (objects, sub_prefixes, reached_end) =
                    self.clip_page_to_range(&task.range, page.objects, page.sub_prefixes);

                listed_keys += objects.len() as u64;
                let last_key: Option<String> = objects.last().map(|e| e.key().to_string());
                let consider_split = last_key.is_some()
                    && page.is_truncated
                    && !reached_end
                    && self.should_split_range(&task, listed_keys, next_split_at);
                // Only materialised when a split is on the table.
                let page_keys: Vec<String> = if consider_split {
                    objects.iter().map(|e| e.key().to_string()).collect()
                } else {
                    Vec::new()
                };

                // Send objects at this level
                if self
                    .send_listed_entries(objects, sender, Some(&task.emitted_prefixes))
                    .await?
                {
                    return Ok(());
                }

                // Collect sub-prefixes (discovery only; leaf pages have none)
                if discovery {
                    all_sub_prefixes.extend(sub_prefixes);
                }

                if reached_end || !page.is_truncated {
                    break;
                }

                match mode {
                    ListingMode::Objects => {
                        let next_token = page.continuation_token;
                        if next_token.is_none() {
                            anyhow::bail!(
                                "S3 returned truncated response but no continuation token for s3://{}/{}",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                        if next_token == continuation_token {
                            anyhow::bail!(
                                "S3 returned the same continuation token twice for s3://{}/{}; \
                                 refusing to loop. This is likely a bug in the S3-compatible endpoint.",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                        continuation_token = next_token;
                    }
                    ListingMode::Versions => {
                        let next_key_marker = page.key_marker;
                        let next_version_id_marker = page.version_id_marker;
                        if next_key_marker.is_none() {
                            anyhow::bail!(
                                "S3 returned truncated response but no next key marker for s3://{}/{}",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                        if next_key_marker == key_marker
                            && next_version_id_marker == version_id_marker
                        {
                            anyhow::bail!(
                                "S3 returned the same key/version marker twice for s3://{}/{}; \
                                 refusing to loop. This is likely a bug in the S3-compatible endpoint.",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                        key_marker = next_key_marker;
                        version_id_marker = next_version_id_marker;
                    }
                }

                if !consider_split {
                    continue;
                }
                let last_key = last_key.expect("consider_split requires a key");
                // Where the next sequential page would start. For versions,
                // only trust S3's marker when it points at the last version we
                // saw; if the page ended on a common prefix the marker's
                // meaning is endpoint-specific, so skip splitting this page.
                let cursor = match mode {
                    ListingMode::Objects => KeyRange {
                        start_after: Some(last_key.clone()),
                        version_id_marker: None,
                        end_inclusive: task.range.end_inclusive.clone(),
                    },
                    ListingMode::Versions => {
                        if key_marker.as_deref() != Some(last_key.as_str()) {
                            continue;
                        }
                        KeyRange {
                            start_after: key_marker.clone(),
                            version_id_marker: version_id_marker.clone(),
                            end_inclusive: task.range.end_inclusive.clone(),
                        }
                    }
                };
                next_split_at = listed_keys + u64::from(self.parallel_range_split_threshold);
                // Probes need permits of their own.
                drop(permit.take());
                match self
                    .plan_range_split(mode, prefix, discovery, cursor, &page_keys)
                    .await?
                {
                    Some(ranges) => {
                        // Sub-prefixes sorted after the cursor will be
                        // rediscovered by the range that contains them.
                        all_sub_prefixes.retain(|cp| cp.as_str() < last_key.as_str());
                        range_children = ranges;
                        break;
                    }
                    None => {
                        permit = Some(
                            self.listing_worker_semaphore
                                .clone()
                                .acquire_owned()
                                .await
                                .expect("listing semaphore closed unexpectedly"),
                        );
                    }
                }
            }

            // Release permit before spawning sub-tasks
            drop(permit.take());

            // At max_depth boundary: emit sub-prefixes as CommonPrefix entries
            // instead of recursing into them, mimicking non-recursive listing.
            // Send directly to avoid depth filtering in send_listed_entries.
            if let Some(max_depth) = self.max_depth
                && task.depth + 1 >= max_depth
            {
                for sub_prefix in all_sub_prefixes.drain(..) {
                    if self.cancellation_token.is_cancelled() {
                        return Ok(());
                    }
                    if sender
                        .send(ListEntry::CommonPrefix(sub_prefix))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }

            if all_sub_prefixes.is_empty() && range_children.is_empty() {
                return Ok(());
            }

            // Spawn sub-tasks: one per discovered sub-prefix, one per key range
            let mut join_set = JoinSet::new();
            let children = all_sub_prefixes
                .into_iter()
                .map(|sub_prefix| ListTask {
                    prefix: Some(sub_prefix),
                    depth: task.depth + 1,
                    range: KeyRange::default(),
                    split_depth: 0,
                    emitted_prefixes: Arc::new(Mutex::new(HashSet::new())),
                })
                .chain(range_children.into_iter().map(|range| ListTask {
                    prefix: task.prefix.clone(),
                    depth: task.depth,
                    range,
                    split_depth: task.split_depth + 1,
                    emitted_prefixes: task.emitted_prefixes.clone(),
                }));

            for child in children {
                let engine = self.clone();
                let sender = sender.clone();
                let sem = self.listing_worker_semaphore.clone();

                join_set.spawn(async move {
                    let sub_permit = sem
                        .acquire_owned()
                        .await
                        .expect("listing semaphore closed unexpectedly");
                    engine
                        .list_with_parallel(mode, &sender, max_keys, child, sub_permit)
                        .await
                });
            }

            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        self.cancellation_token.cancel();
                        return Err(e);
                    }
                    Err(join_err) => {
                        self.cancellation_token.cancel();
                        return Err(anyhow::anyhow!("Listing sub-task panicked: {}", join_err));
                    }
                }
            }

            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// S3Storage — public interface wrapping ListingEngine<S3PageFetcher>
// ---------------------------------------------------------------------------

/// S3-backed implementation of [`StorageTrait`].
#[derive(Clone)]
pub struct S3Storage {
    engine: ListingEngine<S3PageFetcher>,
}

impl S3Storage {
    /// Create a new `S3Storage`.
    ///
    /// When `recursive` is `false`, a delimiter of `"/"` is used so that S3
    /// returns common prefixes (virtual directories). When `recursive` is
    /// `true`, no delimiter is set and all objects under the prefix are
    /// returned.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        client_config: &ClientConfig,
        bucket: String,
        prefix: Option<String>,
        recursive: bool,
        cancellation_token: PipelineCancellationToken,
        request_payer: Option<RequestPayer>,
        max_parallel_listings: u16,
        max_parallel_listing_max_depth: u16,
        max_depth: Option<u16>,
        allow_parallel_listings_in_express_one_zone: bool,
        fetch_owner: bool,
        fetch_restore_status: bool,
        rate_limit_api: Option<u32>,
        parallel_range_split_threshold: u32,
    ) -> Self {
        let client = client_config.create_client().await;
        let delimiter = if recursive {
            None
        } else {
            Some("/".to_string())
        };

        let semaphore_size = max_parallel_listings.max(1) as usize;

        let rate_limiter = rate_limit_api.map(|rate_limit_value| {
            let (interval_ms, refill) = rate_limiter_schedule(rate_limit_value);
            Arc::new(
                RateLimiter::builder()
                    .max(rate_limit_value as usize)
                    .initial(rate_limit_value as usize)
                    .interval(Duration::from_millis(interval_ms))
                    .refill(refill)
                    .fair(true)
                    .build(),
            )
        });

        let fetcher = S3PageFetcher {
            client,
            bucket: bucket.clone(),
            request_payer,
            fetch_owner,
            fetch_restore_status,
        };

        let engine = ListingEngine {
            fetcher,
            bucket,
            prefix,
            delimiter,
            cancellation_token,
            max_parallel_listings,
            max_parallel_listing_max_depth,
            allow_parallel_listings_in_express_one_zone,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(semaphore_size)),
            max_depth,
            rate_limiter,
            api_call_counter: Arc::new(AtomicU64::new(0)),
            parallel_range_split_threshold,
            range_split_unsupported: Arc::new(AtomicBool::new(false)),
            range_split_counter: Arc::new(AtomicU64::new(0)),
        };

        Self { engine }
    }
}

#[async_trait]
impl StorageTrait for S3Storage {
    async fn list_objects(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()> {
        self.engine
            .list_dispatch(ListingMode::Objects, sender, max_keys)
            .await
    }

    async fn list_object_versions(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()> {
        self.engine
            .list_dispatch(ListingMode::Versions, sender, max_keys)
            .await
    }

    fn api_call_count(&self) -> u64 {
        self.engine.api_call_counter.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Choose a `(interval_ms, refill)` pair for a leaky-bucket rate limiter that
/// sustains *exactly* `rate` tokens per second.
///
/// `leaky-bucket` adds `refill` tokens every `interval`, so the sustained rate
/// is `refill * 1000 / interval_ms`. To hit `rate` exactly we pick the smallest
/// interval that divides 1000ms and yields a whole-number refill. This keeps
/// common rates smooth (many small refills spread across the second) while
/// awkward rates — e.g. primes coprime to 1000 like 19 — fall back to a single
/// `rate`-token refill once per second. The previous implementation used a
/// fixed `rate / 10` refill, which floored the effective rate to a multiple of
/// 10 (e.g. `--rate-limit-api 19` throttled to 10/s).
///
/// The returned interval is always >= 1ms and the refill always >= 1, so the
/// builder's non-zero invariants hold for every `rate` in the CLI's `10..`
/// range.
fn rate_limiter_schedule(rate: u32) -> (u64, usize) {
    // Divisors of 1000, smallest (smoothest) first. 1000 itself always
    // divides `rate * interval_ms`, so the loop always returns.
    const INTERVALS_MS: [u64; 16] = [
        1, 2, 4, 5, 8, 10, 20, 25, 40, 50, 100, 125, 200, 250, 500, 1000,
    ];
    let rate = rate as u64;
    for &interval_ms in &INTERVALS_MS {
        let tokens = rate * interval_ms;
        if tokens.is_multiple_of(1000) {
            return (interval_ms, (tokens / 1000) as usize);
        }
    }
    // Unreachable (interval_ms == 1000 always satisfies the check above), but
    // fall back to an exact one-second refill rather than panic.
    (1000, rate as usize)
}

/// Extract error code and message from an AWS SDK error.
fn extract_sdk_error_details<
    E: std::fmt::Display + std::error::Error + ProvideErrorMetadata + 'static,
>(
    e: &SdkError<E>,
) -> (String, String) {
    if let Some(service_err) = e.as_service_error() {
        (
            service_err.code().unwrap_or("unknown").to_string(),
            service_err.message().unwrap_or("no message").to_string(),
        )
    } else {
        ("N/A".to_string(), DisplayErrorContext(e).to_string())
    }
}

/// Convert an AWS SDK `Object` into a [`ListEntry`].
fn convert_object(object: &aws_sdk_s3::types::Object) -> Option<ListEntry> {
    let key = object.key()?.to_string();
    let size = object.size().unwrap_or(0).max(0) as u64;
    let last_modified = aws_datetime_to_chrono(object.last_modified())?;
    let e_tag = object.e_tag().unwrap_or_default().to_string();
    let storage_class = object.storage_class().map(|sc| sc.as_str().to_string());
    let checksum_algorithm: Vec<String> = object
        .checksum_algorithm()
        .iter()
        .map(|a| a.as_str().to_string())
        .collect();
    let checksum_type = object.checksum_type().map(|ct| ct.as_str().to_string());
    let owner_display_name = object
        .owner()
        .and_then(|o| o.display_name())
        .map(|s| s.to_string());
    let owner_id = object.owner().and_then(|o| o.id()).map(|s| s.to_string());
    let is_restore_in_progress = object
        .restore_status()
        .and_then(|rs| rs.is_restore_in_progress());
    let restore_expiry_date = object
        .restore_status()
        .and_then(|rs| rs.restore_expiry_date())
        .and_then(|dt| aws_datetime_to_chrono(Some(dt)))
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));

    Some(ListEntry::Object(S3Object {
        key,
        size,
        last_modified,
        e_tag,
        storage_class,
        checksum_algorithm,
        checksum_type,
        owner_display_name,
        owner_id,
        is_restore_in_progress,
        restore_expiry_date,
        version_info: None,
    }))
}

/// Convert an AWS SDK `ObjectVersion` into a [`ListEntry`].
fn convert_object_version(version: &aws_sdk_s3::types::ObjectVersion) -> Option<ListEntry> {
    let key = version.key()?.to_string();
    let version_id = version.version_id().unwrap_or("null").to_string();
    let size = version.size().unwrap_or(0).max(0) as u64;
    let last_modified = aws_datetime_to_chrono(version.last_modified())?;
    let e_tag = version.e_tag().unwrap_or_default().to_string();
    let is_latest = version.is_latest().unwrap_or(false);
    let storage_class = version.storage_class().map(|sc| sc.as_str().to_string());
    let checksum_algorithm: Vec<String> = version
        .checksum_algorithm()
        .iter()
        .map(|a| a.as_str().to_string())
        .collect();
    let checksum_type = version.checksum_type().map(|ct| ct.as_str().to_string());
    let owner_display_name = version
        .owner()
        .and_then(|o| o.display_name())
        .map(|s| s.to_string());
    let owner_id = version.owner().and_then(|o| o.id()).map(|s| s.to_string());
    let is_restore_in_progress = version
        .restore_status()
        .and_then(|rs| rs.is_restore_in_progress());
    let restore_expiry_date = version
        .restore_status()
        .and_then(|rs| rs.restore_expiry_date())
        .and_then(|dt| aws_datetime_to_chrono(Some(dt)))
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));

    Some(ListEntry::Object(S3Object {
        key,
        size,
        last_modified,
        e_tag,
        storage_class,
        checksum_algorithm,
        checksum_type,
        owner_display_name,
        owner_id,
        is_restore_in_progress,
        restore_expiry_date,
        version_info: Some(VersionInfo {
            version_id,
            is_latest,
        }),
    }))
}

/// Convert an AWS SDK `DeleteMarkerEntry` into a [`ListEntry`].
fn convert_delete_marker(marker: &aws_sdk_s3::types::DeleteMarkerEntry) -> Option<ListEntry> {
    let key = marker.key()?.to_string();
    let version_id = marker.version_id().unwrap_or("null").to_string();
    let last_modified = aws_datetime_to_chrono(marker.last_modified())?;
    let is_latest = marker.is_latest().unwrap_or(false);
    let owner_display_name = marker
        .owner()
        .and_then(|o| o.display_name())
        .map(str::to_string);
    let owner_id = marker.owner().and_then(|o| o.id()).map(str::to_string);

    Some(ListEntry::DeleteMarker {
        key,
        version_info: VersionInfo {
            version_id,
            is_latest,
        },
        last_modified,
        owner_display_name,
        owner_id,
    })
}

/// Convert an AWS SDK `DateTime` to a `chrono::DateTime<Utc>`.
fn aws_datetime_to_chrono(dt: Option<&aws_smithy_types::DateTime>) -> Option<DateTime<Utc>> {
    let dt = dt?;
    let epoch_secs = dt.secs();
    let subsec_nanos = dt.subsec_nanos();
    DateTime::from_timestamp(epoch_secs, subsec_nanos)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::token::create_pipeline_cancellation_token;
    use proptest::prelude::*;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // MockPageFetcher
    // -----------------------------------------------------------------------

    type PageMap = HashMap<(Option<String>, Option<String>), Vec<ListPage>>;

    /// Production default; the page-map mocks never list this many keys, so
    /// range splitting stays inert unless a test opts in explicitly.
    const TEST_RANGE_SPLIT_THRESHOLD: u32 = 5000;

    /// A mock page fetcher that returns pre-configured pages keyed by
    /// (prefix, delimiter) so parallel listing can get different results
    /// for different sub-prefixes.
    #[derive(Clone)]
    struct MockPageFetcher {
        /// Key: (prefix, delimiter) -> queue of pages to return in order.
        pages: Arc<Mutex<PageMap>>,
    }

    impl MockPageFetcher {
        /// Create a mock that returns the given pages for the default prefix.
        fn from_pages(prefix: Option<&str>, delimiter: Option<&str>, pages: Vec<ListPage>) -> Self {
            let mut map = HashMap::new();
            map.insert(
                (
                    prefix.map(|s| s.to_string()),
                    delimiter.map(|s| s.to_string()),
                ),
                pages,
            );
            Self {
                pages: Arc::new(Mutex::new(map)),
            }
        }

        /// Create a mock with a full map of (prefix, delimiter) -> pages.
        fn from_map(map: PageMap) -> Self {
            Self {
                pages: Arc::new(Mutex::new(map)),
            }
        }
    }

    #[async_trait]
    impl PageFetcher for MockPageFetcher {
        async fn fetch_page(
            &self,
            _mode: ListingMode,
            _max_keys: i32,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            _continuation_token: Option<&str>,
            _key_marker: Option<&str>,
            _version_id_marker: Option<&str>,
            _start_after: Option<&str>,
        ) -> Result<ListPage> {
            let key = (
                prefix.map(|s| s.to_string()),
                delimiter.map(|s| s.to_string()),
            );
            let mut map = self.pages.lock().unwrap();
            if let Some(queue) = map.get_mut(&key)
                && !queue.is_empty()
            {
                return Ok(queue.remove(0));
            }
            // Default: empty non-truncated page
            Ok(ListPage {
                objects: vec![],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            })
        }
    }

    /// A mock that always returns an error (used for error-propagation tests).
    #[derive(Clone)]
    struct ErrorPageFetcher {
        /// Which prefix triggers the error. None means all prefixes error.
        error_prefix: Option<String>,
        /// Fallback fetcher for non-error prefixes.
        fallback: MockPageFetcher,
    }

    #[async_trait]
    impl PageFetcher for ErrorPageFetcher {
        async fn fetch_page(
            &self,
            mode: ListingMode,
            max_keys: i32,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            continuation_token: Option<&str>,
            key_marker: Option<&str>,
            version_id_marker: Option<&str>,
            start_after: Option<&str>,
        ) -> Result<ListPage> {
            if let Some(ref err_prefix) = self.error_prefix {
                if prefix == Some(err_prefix.as_str()) {
                    anyhow::bail!("simulated S3 error for prefix {}", err_prefix);
                }
            } else {
                anyhow::bail!("simulated S3 error");
            }
            self.fallback
                .fetch_page(
                    mode,
                    max_keys,
                    prefix,
                    delimiter,
                    continuation_token,
                    key_marker,
                    version_id_marker,
                    start_after,
                )
                .await
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_entry(key: &str) -> ListEntry {
        ListEntry::Object(S3Object {
            key: key.to_string(),
            size: 100,
            last_modified: chrono::Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        })
    }

    fn make_engine<F: PageFetcher + Clone + 'static>(
        fetcher: F,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_parallel: u16,
        max_depth: u16,
        allow_express: bool,
    ) -> ListingEngine<F> {
        let token = create_pipeline_cancellation_token();
        make_engine_with_token(
            fetcher,
            bucket,
            prefix,
            delimiter,
            max_parallel,
            max_depth,
            allow_express,
            token,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn make_engine_with_token<F: PageFetcher + Clone + 'static>(
        fetcher: F,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_parallel: u16,
        max_depth: u16,
        allow_express: bool,
        token: PipelineCancellationToken,
    ) -> ListingEngine<F> {
        ListingEngine {
            fetcher,
            bucket: bucket.to_string(),
            prefix: prefix.map(|s| s.to_string()),
            delimiter: delimiter.map(|s| s.to_string()),
            cancellation_token: token,
            max_parallel_listings: max_parallel,
            max_parallel_listing_max_depth: max_depth,
            allow_parallel_listings_in_express_one_zone: allow_express,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(
                max_parallel.max(1) as usize
            )),
            max_depth: None,
            rate_limiter: None,
            api_call_counter: Arc::new(AtomicU64::new(0)),
            parallel_range_split_threshold: TEST_RANGE_SPLIT_THRESHOLD,
            range_split_unsupported: Arc::new(AtomicBool::new(false)),
            range_split_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn collect_entries(
        engine: &ListingEngine<impl PageFetcher + Clone + 'static>,
        mode: ListingMode,
        max_keys: i32,
    ) -> Result<Vec<ListEntry>> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let result = engine.list_dispatch(mode, &tx, max_keys).await;
        drop(tx);
        let mut entries = Vec::new();
        while let Some(e) = rx.recv().await {
            entries.push(e);
        }
        result.map(|()| entries)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    // 1. dispatch_uses_sequential_when_non_recursive
    #[tokio::test]
    async fn dispatch_uses_sequential_when_non_recursive() {
        // delimiter is set -> sequential, even with max_parallel > 1
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            Some("/"),
            vec![ListPage {
                objects: vec![make_entry("prefix/file.txt")],
                sub_prefixes: vec!["prefix/sub/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), Some("/"), 4, 3, false);

        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        // Should get the object + the common prefix
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key(), "prefix/file.txt");
        assert_eq!(entries[1].key(), "prefix/sub/");
    }

    // 2. dispatch_uses_sequential_when_max_parallel_is_1
    #[tokio::test]
    async fn dispatch_uses_sequential_when_max_parallel_is_1() {
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None, // no delimiter in sequential call when recursive
            vec![ListPage {
                objects: vec![make_entry("prefix/a.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        // recursive (no delimiter) + max_parallel=1 => sequential
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 1, 3, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key(), "prefix/a.txt");
    }

    // 3. dispatch_uses_parallel_when_recursive_and_multiple_workers
    #[tokio::test]
    async fn dispatch_uses_parallel_when_recursive_and_multiple_workers() {
        // Parallel listing first fetches with delimiter "/" at the top level,
        // then recurses into sub-prefixes.
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top-level: returns sub-prefixes a/ and b/, plus one object at root
        map.insert(
            (Some("prefix/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("prefix/root.txt")],
                sub_prefixes: vec!["prefix/a/".to_string(), "prefix/b/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Sub-prefix a/
        map.insert(
            (Some("prefix/a/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("prefix/a/1.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Sub-prefix b/
        map.insert(
            (Some("prefix/b/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("prefix/b/2.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        // recursive (no delimiter) + max_parallel=4 => parallel
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 4, 3, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["prefix/a/1.txt", "prefix/b/2.txt", "prefix/root.txt"]
        );
    }

    // 4. dispatch_uses_sequential_for_express_one_zone
    #[tokio::test]
    async fn dispatch_uses_sequential_for_express_one_zone() {
        // Bucket ends with --x-s3, allow_express=false => sequential
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![ListPage {
                objects: vec![make_entry("prefix/express.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        // Note: sequential with recursive (no delimiter in engine) will call with delimiter=None
        let engine = make_engine(
            fetcher,
            "my-bucket--x-s3",
            Some("prefix/"),
            None,
            4,
            3,
            false,
        );
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key(), "prefix/express.txt");
    }

    // 5. dispatch_uses_parallel_for_express_one_zone_when_allowed
    #[tokio::test]
    async fn dispatch_uses_parallel_for_express_one_zone_when_allowed() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();
        map.insert(
            (Some("prefix/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("prefix/express.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let fetcher = MockPageFetcher::from_map(map);
        // Express bucket + allow=true + recursive + parallel>1 => parallel
        let engine = make_engine(
            fetcher,
            "my-bucket--x-s3",
            Some("prefix/"),
            None,
            4,
            3,
            true,
        );
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key(), "prefix/express.txt");
    }

    // 6. sequential_paginates_through_multiple_pages
    #[tokio::test]
    async fn sequential_paginates_through_multiple_pages() {
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![
                ListPage {
                    objects: vec![make_entry("prefix/1.txt")],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: Some("token1".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
                ListPage {
                    objects: vec![make_entry("prefix/2.txt")],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: Some("token2".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
                ListPage {
                    objects: vec![make_entry("prefix/3.txt")],
                    sub_prefixes: vec![],
                    is_truncated: false,
                    continuation_token: None,
                    key_marker: None,
                    version_id_marker: None,
                },
            ],
        );
        // max_parallel=1 => sequential
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 1, 3, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        let keys: Vec<&str> = entries.iter().map(|e| e.key()).collect();
        assert_eq!(keys, vec!["prefix/1.txt", "prefix/2.txt", "prefix/3.txt"]);
    }

    // 7. sequential_stops_on_cancellation
    #[tokio::test]
    async fn sequential_stops_on_cancellation() {
        // Cancel the token before listing starts => no pages should be fetched at all
        let token = create_pipeline_cancellation_token();
        token.cancel();

        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![ListPage {
                objects: vec![make_entry("prefix/1.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let engine =
            make_engine_with_token(fetcher, "bucket", Some("prefix/"), None, 1, 3, false, token);

        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        // Should have received no entries because token was already cancelled
        assert!(entries.is_empty());
    }

    // 8. sequential_errors_on_truncated_without_token
    #[tokio::test]
    async fn sequential_errors_on_truncated_without_token() {
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![ListPage {
                objects: vec![make_entry("prefix/1.txt")],
                sub_prefixes: vec![],
                is_truncated: true,
                continuation_token: None, // missing!
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 1, 3, false);
        let result = collect_entries(&engine, ListingMode::Objects, 1000).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("truncated") && err_msg.contains("continuation token"),
            "unexpected error: {}",
            err_msg
        );
    }

    // 8b. sequential_errors_on_duplicate_continuation_token
    #[tokio::test]
    async fn sequential_errors_on_duplicate_continuation_token() {
        // A buggy S3-compatible endpoint that returns the same continuation
        // token twice should be detected and aborted, not looped on forever.
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![
                ListPage {
                    objects: vec![make_entry("prefix/1.txt")],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: Some("stuck-token".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
                ListPage {
                    objects: vec![make_entry("prefix/2.txt")],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: Some("stuck-token".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
            ],
        );
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 1, 3, false);
        let result = collect_entries(&engine, ListingMode::Objects, 1000).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("same continuation token"),
            "unexpected error: {}",
            err_msg
        );
    }

    // 8d. parallel_errors_on_duplicate_continuation_token
    #[tokio::test]
    async fn parallel_errors_on_duplicate_continuation_token() {
        // Same defense as 8b but for the parallel prefix-discovery loop in
        // list_with_parallel. Parallel pagination is at (prefix, Some("/")).
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![
                ListPage {
                    objects: vec![],
                    sub_prefixes: vec!["p/a/".to_string()],
                    is_truncated: true,
                    continuation_token: Some("stuck-token".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
                ListPage {
                    objects: vec![],
                    sub_prefixes: vec!["p/b/".to_string()],
                    is_truncated: true,
                    continuation_token: Some("stuck-token".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
            ],
        );
        let fetcher = MockPageFetcher::from_map(map);
        // recursive (no delimiter) + max_parallel > 1 => parallel
        let engine = make_engine(fetcher, "bucket", Some("p/"), None, 4, 3, false);
        let result = collect_entries(&engine, ListingMode::Objects, 1000).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("same continuation token"),
            "unexpected error: {}",
            err_msg
        );
    }

    // 8e. parallel_errors_on_duplicate_versions_marker
    #[tokio::test]
    async fn parallel_errors_on_duplicate_versions_marker() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![
                ListPage {
                    objects: vec![],
                    sub_prefixes: vec!["p/a/".to_string()],
                    is_truncated: true,
                    continuation_token: None,
                    key_marker: Some("k".to_string()),
                    version_id_marker: Some("v".to_string()),
                },
                ListPage {
                    objects: vec![],
                    sub_prefixes: vec!["p/b/".to_string()],
                    is_truncated: true,
                    continuation_token: None,
                    key_marker: Some("k".to_string()),
                    version_id_marker: Some("v".to_string()),
                },
            ],
        );
        let fetcher = MockPageFetcher::from_map(map);
        let engine = make_engine(fetcher, "bucket", Some("p/"), None, 4, 3, false);
        let result = collect_entries(&engine, ListingMode::Versions, 1000).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("same key/version marker"),
            "unexpected error: {}",
            err_msg
        );
    }

    // 8c. sequential_errors_on_duplicate_versions_marker
    #[tokio::test]
    async fn sequential_errors_on_duplicate_versions_marker() {
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![
                ListPage {
                    objects: vec![make_entry("prefix/1.txt")],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: None,
                    key_marker: Some("k".to_string()),
                    version_id_marker: Some("v".to_string()),
                },
                ListPage {
                    objects: vec![make_entry("prefix/2.txt")],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: None,
                    key_marker: Some("k".to_string()),
                    version_id_marker: Some("v".to_string()),
                },
            ],
        );
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 1, 3, false);
        let result = collect_entries(&engine, ListingMode::Versions, 1000).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("same key/version marker"),
            "unexpected error: {}",
            err_msg
        );
    }

    // 9. parallel_discovers_sub_prefixes_and_lists_them
    #[tokio::test]
    async fn parallel_discovers_sub_prefixes_and_lists_them() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level with delimiter
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/a/".to_string(), "p/b/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Sub-prefix a/ with delimiter
        map.insert(
            (Some("p/a/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/a/file1.txt"), make_entry("p/a/file2.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Sub-prefix b/ with delimiter
        map.insert(
            (Some("p/b/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/b/file3.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        let engine = make_engine(fetcher, "bucket", Some("p/"), None, 4, 5, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["p/a/file1.txt", "p/a/file2.txt", "p/b/file3.txt"]
        );
    }

    // 10. parallel_falls_back_to_sequential_beyond_max_depth
    #[tokio::test]
    async fn parallel_falls_back_to_sequential_beyond_max_depth() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level (depth 0) with delimiter
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/deep/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // depth 1 with delimiter
        map.insert(
            (Some("p/deep/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/deep/deeper/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // depth 2 > max_depth(1) => falls back to sequential with NO delimiter
        // So the call will be (prefix="p/deep/deeper/", delimiter=None)
        map.insert(
            (Some("p/deep/deeper/".to_string()), None),
            vec![ListPage {
                objects: vec![
                    make_entry("p/deep/deeper/file.txt"),
                    make_entry("p/deep/deeper/sub/file2.txt"),
                ],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        // max_depth = 1, so depth 2 (> 1) falls back to sequential
        let engine = make_engine(fetcher, "bucket", Some("p/"), None, 4, 1, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["p/deep/deeper/file.txt", "p/deep/deeper/sub/file2.txt"]
        );
    }

    // 11. parallel_cancels_on_sub_task_error
    #[tokio::test]
    async fn parallel_cancels_on_sub_task_error() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level discovers two sub-prefixes
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/ok/".to_string(), "p/fail/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // p/ok/ succeeds
        map.insert(
            (Some("p/ok/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/ok/file.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fallback = MockPageFetcher::from_map(map);
        let fetcher = ErrorPageFetcher {
            error_prefix: Some("p/fail/".to_string()),
            fallback,
        };

        let token = create_pipeline_cancellation_token();
        let token_check = token.clone();
        let engine =
            make_engine_with_token(fetcher, "bucket", Some("p/"), None, 4, 5, false, token);

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let result = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        drop(tx);

        // Drain channel
        let mut entries = Vec::new();
        while let Some(e) = rx.recv().await {
            entries.push(e);
        }

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("simulated S3 error"),
            "unexpected error: {}",
            err_msg
        );
        // Token should have been cancelled
        assert!(token_check.is_cancelled());
    }

    // Helper: make_engine_with_max_depth — allows setting content max_depth
    #[allow(clippy::too_many_arguments)]
    fn make_engine_with_max_depth<F: PageFetcher + Clone + 'static>(
        fetcher: F,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_parallel: u16,
        max_depth: u16,
        allow_express: bool,
        content_max_depth: Option<u16>,
    ) -> ListingEngine<F> {
        let token = create_pipeline_cancellation_token();
        ListingEngine {
            fetcher,
            bucket: bucket.to_string(),
            prefix: prefix.map(|s| s.to_string()),
            delimiter: delimiter.map(|s| s.to_string()),
            cancellation_token: token,
            max_parallel_listings: max_parallel,
            max_parallel_listing_max_depth: max_depth,
            allow_parallel_listings_in_express_one_zone: allow_express,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(
                max_parallel.max(1) as usize
            )),
            max_depth: content_max_depth,
            rate_limiter: None,
            api_call_counter: Arc::new(AtomicU64::new(0)),
            parallel_range_split_threshold: TEST_RANGE_SPLIT_THRESHOLD,
            range_split_unsupported: Arc::new(AtomicBool::new(false)),
            range_split_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    // 12. parallel_respects_max_depth
    #[tokio::test]
    async fn parallel_respects_max_depth() {
        // Structure: p/ -> p/a/ -> p/a/deep/ -> p/a/deep/file.txt
        // With max_depth=2, objects at key-depth 1 and 2 appear.
        // p/a/deep/ (key-depth 3) should NOT be fetched.
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level (depth 0): discovers sub-prefix a/
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/root.txt")],
                sub_prefixes: vec!["p/a/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Depth 1 (a/): discovers sub-prefix deep/, has direct objects
        map.insert(
            (Some("p/a/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/a/file.txt")],
                sub_prefixes: vec!["p/a/deep/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Depth 2 (deep/) — should NOT be reached with max_depth=2
        map.insert(
            (Some("p/a/deep/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/a/deep/should_not_appear.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        // max_depth=2: include key-depth 1 (p/root.txt) and 2 (p/a/file.txt)
        let engine =
            make_engine_with_max_depth(fetcher, "bucket", Some("p/"), None, 4, 5, false, Some(2));
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        // p/a/deep/ emitted as CommonPrefix at the max_depth boundary
        assert_eq!(keys, vec!["p/a/deep/", "p/a/file.txt", "p/root.txt"]);
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, ListEntry::CommonPrefix(p) if p == "p/a/deep/"))
        );
    }

    // 13. sequential_emits_common_prefix_at_max_depth_boundary
    #[tokio::test]
    async fn sequential_emits_common_prefix_at_max_depth_boundary() {
        // Sequential listing with max_depth filtering.
        // Prefix is "p/", max_depth=1. Objects beyond depth 1 become CommonPrefix entries.
        let fetcher = MockPageFetcher::from_pages(
            Some("p/"),
            None,
            vec![ListPage {
                objects: vec![
                    make_entry("p/file1.txt"),     // depth 1 — include as object
                    make_entry("p/a/file2.txt"),   // depth 2 — becomes CommonPrefix "p/a/"
                    make_entry("p/a/b/file3.txt"), // depth 3 — same CommonPrefix "p/a/" (deduped)
                ],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine =
            make_engine_with_max_depth(fetcher, "bucket", Some("p/"), None, 1, 3, false, Some(1));
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        // p/a/file2.txt and p/a/b/file3.txt both collapse into one "p/a/" prefix
        assert_eq!(keys, vec!["p/a/", "p/file1.txt"]);
        // Verify the CommonPrefix entry type
        assert!(
            entries
                .iter()
                .any(|e| matches!(e, ListEntry::CommonPrefix(p) if p == "p/a/"))
        );
    }

    // 14. sequential_max_depth_with_no_prefix
    #[tokio::test]
    async fn sequential_max_depth_with_no_prefix() {
        let fetcher = MockPageFetcher::from_pages(
            None,
            None,
            vec![ListPage {
                objects: vec![
                    make_entry("file.txt"),     // depth 1 — include
                    make_entry("a/file.txt"),   // depth 2 — becomes CommonPrefix "a/"
                    make_entry("a/b/file.txt"), // depth 3 — same CommonPrefix "a/" (deduped)
                ],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine =
            make_engine_with_max_depth(fetcher, "bucket", None, None, 1, 3, false, Some(1));
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["a/", "file.txt"]);
    }

    // 15. parallel_fallback_to_sequential_respects_max_depth
    #[tokio::test]
    async fn parallel_fallback_to_sequential_respects_max_depth() {
        // max_parallel_listing_max_depth=0, so depth 1 immediately falls back to sequential.
        // max_depth=2, so sequential must filter objects beyond depth 2.
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level (depth 0): discovers sub-prefix a/
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/root.txt")],
                sub_prefixes: vec!["p/a/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Depth 1 > max_parallel_listing_max_depth(0) => sequential with delimiter=None
        // Sequential returns everything under p/a/ including deep objects
        map.insert(
            (Some("p/a/".to_string()), None),
            vec![ListPage {
                objects: vec![
                    make_entry("p/a/file.txt"),     // depth 2 — include
                    make_entry("p/a/b/file.txt"),   // depth 3 — exclude
                    make_entry("p/a/b/c/file.txt"), // depth 4 — exclude
                ],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        let token = create_pipeline_cancellation_token();
        let engine = ListingEngine {
            fetcher,
            bucket: "bucket".to_string(),
            prefix: Some("p/".to_string()),
            delimiter: None,
            cancellation_token: token,
            max_parallel_listings: 4,
            max_parallel_listing_max_depth: 0,
            max_depth: Some(2),
            allow_parallel_listings_in_express_one_zone: false,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(4)),
            rate_limiter: None,
            api_call_counter: Arc::new(AtomicU64::new(0)),
            parallel_range_split_threshold: TEST_RANGE_SPLIT_THRESHOLD,
            range_split_unsupported: Arc::new(AtomicBool::new(false)),
            range_split_counter: Arc::new(AtomicU64::new(0)),
        };
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        // p/a/b/file.txt and p/a/b/c/file.txt collapse into CommonPrefix "p/a/b/"
        assert_eq!(keys, vec!["p/a/b/", "p/a/file.txt", "p/root.txt"]);
    }

    // ========================================================================
    // key_depth unit tests (including directory-marker edge case)
    // ========================================================================

    fn engine_with_prefix(prefix: Option<&str>) -> ListingEngine<MockPageFetcher> {
        let fetcher = MockPageFetcher::from_pages(prefix, None, vec![]);
        make_engine(fetcher, "bucket", prefix, None, 1, 3, false)
    }

    #[test]
    fn key_depth_regular_objects() {
        let engine = engine_with_prefix(Some("p/"));
        assert_eq!(engine.key_depth("p/file.txt"), Some(1));
        assert_eq!(engine.key_depth("p/a/file.txt"), Some(2));
        assert_eq!(engine.key_depth("p/a/b/file.txt"), Some(3));
    }

    #[test]
    fn key_depth_directory_markers_match_regular_objects() {
        // Regression: directory-marker objects (keys ending in "/") should
        // be counted at the same depth as regular objects in the same
        // "folder", not one level deeper.
        let engine = engine_with_prefix(Some("p/"));
        // "p/sub/" is at the top level under "p/" — same depth as
        // "p/file.txt"
        assert_eq!(engine.key_depth("p/sub/"), Some(1));
        // "p/a/sub/" is one level down — same depth as "p/a/file.txt"
        assert_eq!(engine.key_depth("p/a/sub/"), Some(2));
    }

    #[test]
    fn key_depth_key_equals_prefix_returns_none() {
        let engine = engine_with_prefix(Some("p/"));
        assert_eq!(engine.key_depth("p/"), None);
    }

    #[test]
    fn key_depth_key_not_under_prefix_returns_none() {
        let engine = engine_with_prefix(Some("p/"));
        assert_eq!(engine.key_depth("q/file.txt"), None);
    }

    #[test]
    fn key_depth_empty_prefix() {
        let engine = engine_with_prefix(None);
        assert_eq!(engine.key_depth("file.txt"), Some(1));
        assert_eq!(engine.key_depth("a/file.txt"), Some(2));
        assert_eq!(engine.key_depth("sub/"), Some(1)); // directory marker at top
    }

    // -----------------------------------------------------------------------
    // rate_limiter_schedule: exact sustained rate
    // -----------------------------------------------------------------------

    #[test]
    fn rate_limiter_schedule_is_exact_for_all_valid_rates() {
        // The CLI accepts --rate-limit-api values of 10 and up. For every one,
        // the sustained rate refill * (1000 / interval_ms) must equal the
        // requested rate exactly, i.e. refill * 1000 == rate * interval_ms.
        for rate in 10u32..=65_535 {
            let (interval_ms, refill) = rate_limiter_schedule(rate);
            assert!(interval_ms >= 1, "interval must be non-zero (rate {rate})");
            assert!(interval_ms <= 1000, "interval must be <= 1s (rate {rate})");
            assert!(refill >= 1, "refill must be non-zero (rate {rate})");
            assert_eq!(
                refill as u64 * 1000,
                rate as u64 * interval_ms,
                "schedule not exact for rate {rate}: refill={refill}, interval_ms={interval_ms}"
            );
        }
    }

    #[test]
    fn rate_limiter_schedule_prefers_smooth_intervals_for_round_rates() {
        // "Round" rates get a sub-second interval with a small refill, so
        // tokens spread across the second instead of arriving in one burst.
        assert_eq!(rate_limiter_schedule(10), (100, 1));
        assert_eq!(rate_limiter_schedule(50), (20, 1));
        assert_eq!(rate_limiter_schedule(100), (10, 1));
        assert_eq!(rate_limiter_schedule(1000), (1, 1));
    }

    #[test]
    fn rate_limiter_schedule_falls_back_to_one_second_for_awkward_rates() {
        // A rate coprime to 1000 (e.g. 19) can only be hit exactly with a
        // full-second interval refilling `rate` tokens at once. The old
        // `rate / 10` refill floored 19 to an effective 10/s; now it is 19/s.
        assert_eq!(rate_limiter_schedule(19), (1000, 19));
        assert_eq!(rate_limiter_schedule(23), (1000, 23));
    }

    // -----------------------------------------------------------------------
    // Parallel concurrency limit
    // -----------------------------------------------------------------------

    /// A fetcher that records the peak number of concurrent `fetch_page`
    /// calls, sleeping on each call to force overlap. Used to verify the
    /// listing semaphore actually bounds concurrency during leaf (sequential)
    /// scans, not just during prefix discovery.
    #[derive(Clone)]
    struct ConcurrencyTrackingFetcher {
        inner: MockPageFetcher,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl PageFetcher for ConcurrencyTrackingFetcher {
        async fn fetch_page(
            &self,
            mode: ListingMode,
            max_keys: i32,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            continuation_token: Option<&str>,
            key_marker: Option<&str>,
            version_id_marker: Option<&str>,
            start_after: Option<&str>,
        ) -> Result<ListPage> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            // Sleep while "in flight" so concurrent scans actually overlap.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let result = self
                .inner
                .fetch_page(
                    mode,
                    max_keys,
                    prefix,
                    delimiter,
                    continuation_token,
                    key_marker,
                    version_id_marker,
                    start_after,
                )
                .await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }
    }

    // Regression: leaf sequential scans must respect max_parallel_listings.
    // Previously list_with_parallel dropped its semaphore permit *before* the
    // leaf sequential scan, letting an unbounded number of leaf listings run
    // at once. With max_parallel_listings = 2, no more than 2 fetch_page
    // calls may be in flight simultaneously.
    #[tokio::test]
    async fn parallel_leaf_scans_respect_concurrency_limit() {
        let mut map: PageMap = HashMap::new();
        // Root discovery (delimiter "/") reveals 6 leaf prefixes, no objects.
        let leaves = ["p/a/", "p/b/", "p/c/", "p/d/", "p/e/", "p/f/"];
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: leaves.iter().map(|s| s.to_string()).collect(),
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        // Each leaf is scanned sequentially (delimiter None) — one object each.
        for leaf in leaves {
            map.insert(
                (Some(leaf.to_string()), None),
                vec![ListPage {
                    objects: vec![make_entry(&format!("{leaf}obj.txt"))],
                    sub_prefixes: vec![],
                    is_truncated: false,
                    continuation_token: None,
                    key_marker: None,
                    version_id_marker: None,
                }],
            );
        }

        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetcher = ConcurrencyTrackingFetcher {
            inner: MockPageFetcher::from_map(map),
            in_flight,
            max_in_flight: max_in_flight.clone(),
        };

        // max_parallel_listing_max_depth = 0 makes the depth-1 children leaf
        // sequential scans; the semaphore of size 2 must cap them at 2.
        let token = create_pipeline_cancellation_token();
        let engine = ListingEngine {
            fetcher,
            bucket: "bucket".to_string(),
            prefix: Some("p/".to_string()),
            delimiter: None,
            cancellation_token: token,
            max_parallel_listings: 2,
            max_parallel_listing_max_depth: 0,
            allow_parallel_listings_in_express_one_zone: false,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(2)),
            max_depth: None,
            rate_limiter: None,
            api_call_counter: Arc::new(AtomicU64::new(0)),
            parallel_range_split_threshold: TEST_RANGE_SPLIT_THRESHOLD,
            range_split_unsupported: Arc::new(AtomicBool::new(false)),
            range_split_counter: Arc::new(AtomicU64::new(0)),
        };

        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();

        assert_eq!(entries.len(), 6, "expected all leaf objects: {entries:?}");
        let peak = max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak <= 2,
            "peak concurrent fetch_page was {peak}, expected <= 2"
        );
    }

    // -----------------------------------------------------------------------
    // Rate limiter tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn rate_limiter_still_lists_objects() {
        // Verify that rate-limited listing still returns all objects correctly.
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![ListPage {
                objects: vec![
                    ListEntry::Object(S3Object {
                        key: "prefix/a.txt".to_string(),
                        size: 10,
                        last_modified: chrono::Utc::now(),
                        e_tag: "\"e\"".to_string(),
                        storage_class: Some("STANDARD".to_string()),
                        checksum_algorithm: vec![],
                        checksum_type: None,
                        owner_display_name: None,
                        owner_id: None,
                        is_restore_in_progress: None,
                        restore_expiry_date: None,
                        version_info: None,
                    }),
                    ListEntry::Object(S3Object {
                        key: "prefix/b.txt".to_string(),
                        size: 20,
                        last_modified: chrono::Utc::now(),
                        e_tag: "\"e\"".to_string(),
                        storage_class: Some("STANDARD".to_string()),
                        checksum_algorithm: vec![],
                        checksum_type: None,
                        owner_display_name: None,
                        owner_id: None,
                        is_restore_in_progress: None,
                        restore_expiry_date: None,
                        version_info: None,
                    }),
                ],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let token = create_pipeline_cancellation_token();
        let rate_limiter = Arc::new(
            RateLimiter::builder()
                .max(100)
                .initial(100)
                .refill(10)
                .fair(true)
                .build(),
        );
        let engine = ListingEngine {
            fetcher,
            bucket: "bucket".to_string(),
            prefix: Some("prefix/".to_string()),
            delimiter: None,
            cancellation_token: token,
            max_parallel_listings: 1,
            max_parallel_listing_max_depth: 2,
            allow_parallel_listings_in_express_one_zone: false,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            max_depth: None,
            rate_limiter: Some(rate_limiter),
            api_call_counter: Arc::new(AtomicU64::new(0)),
            parallel_range_split_threshold: TEST_RANGE_SPLIT_THRESHOLD,
            range_split_unsupported: Arc::new(AtomicBool::new(false)),
            range_split_counter: Arc::new(AtomicU64::new(0)),
        };

        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key(), "prefix/a.txt");
        assert_eq!(entries[1].key(), "prefix/b.txt");
    }

    #[tokio::test]
    async fn no_rate_limiter_lists_objects() {
        // Verify that listing without rate limiter (None) works identically.
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![ListPage {
                objects: vec![ListEntry::Object(S3Object {
                    key: "prefix/file.txt".to_string(),
                    size: 10,
                    last_modified: chrono::Utc::now(),
                    e_tag: "\"e\"".to_string(),
                    storage_class: Some("STANDARD".to_string()),
                    checksum_algorithm: vec![],
                    checksum_type: None,
                    owner_display_name: None,
                    owner_id: None,
                    is_restore_in_progress: None,
                    restore_expiry_date: None,
                    version_info: None,
                })],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let engine = make_engine_with_token(
            fetcher,
            "bucket",
            Some("prefix/"),
            None,
            1,
            2,
            false,
            create_pipeline_cancellation_token(),
        );

        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key(), "prefix/file.txt");
    }

    #[tokio::test]
    async fn api_call_counter_increments_per_page() {
        // Two pages: first is truncated, second is final.
        let fetcher = MockPageFetcher::from_pages(
            Some("p/"),
            None,
            vec![
                ListPage {
                    objects: vec![ListEntry::Object(S3Object {
                        key: "p/a.txt".to_string(),
                        size: 10,
                        last_modified: chrono::Utc::now(),
                        e_tag: "\"e\"".to_string(),
                        storage_class: Some("STANDARD".to_string()),
                        checksum_algorithm: vec![],
                        checksum_type: None,
                        owner_display_name: None,
                        owner_id: None,
                        is_restore_in_progress: None,
                        restore_expiry_date: None,
                        version_info: None,
                    })],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: Some("token1".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
                ListPage {
                    objects: vec![ListEntry::Object(S3Object {
                        key: "p/b.txt".to_string(),
                        size: 20,
                        last_modified: chrono::Utc::now(),
                        e_tag: "\"e\"".to_string(),
                        storage_class: Some("STANDARD".to_string()),
                        checksum_algorithm: vec![],
                        checksum_type: None,
                        owner_display_name: None,
                        owner_id: None,
                        is_restore_in_progress: None,
                        restore_expiry_date: None,
                        version_info: None,
                    })],
                    sub_prefixes: vec![],
                    is_truncated: false,
                    continuation_token: None,
                    key_marker: None,
                    version_id_marker: None,
                },
            ],
        );

        let token = create_pipeline_cancellation_token();
        let counter = Arc::new(AtomicU64::new(0));
        let engine = ListingEngine {
            fetcher,
            bucket: "bucket".to_string(),
            prefix: Some("p/".to_string()),
            delimiter: None,
            cancellation_token: token,
            max_parallel_listings: 1,
            max_parallel_listing_max_depth: 2,
            allow_parallel_listings_in_express_one_zone: false,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            max_depth: None,
            rate_limiter: None,
            api_call_counter: counter.clone(),
            parallel_range_split_threshold: TEST_RANGE_SPLIT_THRESHOLD,
            range_split_unsupported: Arc::new(AtomicBool::new(false)),
            range_split_counter: Arc::new(AtomicU64::new(0)),
        };

        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            counter.load(Ordering::Relaxed),
            2,
            "expected 2 API calls for 2 pages"
        );
    }

    #[tokio::test]
    async fn api_call_counter_single_page() {
        let fetcher = MockPageFetcher::from_pages(
            Some("p/"),
            None,
            vec![ListPage {
                objects: vec![ListEntry::Object(S3Object {
                    key: "p/file.txt".to_string(),
                    size: 10,
                    last_modified: chrono::Utc::now(),
                    e_tag: "\"e\"".to_string(),
                    storage_class: Some("STANDARD".to_string()),
                    checksum_algorithm: vec![],
                    checksum_type: None,
                    owner_display_name: None,
                    owner_id: None,
                    is_restore_in_progress: None,
                    restore_expiry_date: None,
                    version_info: None,
                })],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let token = create_pipeline_cancellation_token();
        let counter = Arc::new(AtomicU64::new(0));
        let engine = ListingEngine {
            fetcher,
            bucket: "bucket".to_string(),
            prefix: Some("p/".to_string()),
            delimiter: None,
            cancellation_token: token,
            max_parallel_listings: 1,
            max_parallel_listing_max_depth: 2,
            allow_parallel_listings_in_express_one_zone: false,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            max_depth: None,
            rate_limiter: None,
            api_call_counter: counter.clone(),
            parallel_range_split_threshold: TEST_RANGE_SPLIT_THRESHOLD,
            range_split_unsupported: Arc::new(AtomicBool::new(false)),
            range_split_counter: Arc::new(AtomicU64::new(0)),
        };

        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "expected 1 API call for single page"
        );
    }

    // =======================================================================
    // send_listed_entries: cancellation and receiver-dropped signals
    // =======================================================================

    #[tokio::test]
    async fn send_listed_entries_stops_on_cancel() {
        let token = create_pipeline_cancellation_token();
        token.cancel();
        let engine = make_engine_with_token(
            MockPageFetcher::from_map(HashMap::new()),
            "bucket",
            None,
            None,
            1,
            5,
            false,
            token,
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let stop = engine
            .send_listed_entries(vec![make_entry("a.txt")], &tx, None)
            .await
            .unwrap();
        assert!(stop, "cancelled token must signal stop");
    }

    #[tokio::test]
    async fn send_listed_entries_stops_when_receiver_dropped() {
        let engine = make_engine(
            MockPageFetcher::from_map(HashMap::new()),
            "bucket",
            None,
            None,
            1,
            5,
            false,
        );
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let stop = engine
            .send_listed_entries(vec![make_entry("a.txt")], &tx, None)
            .await
            .unwrap();
        assert!(stop, "dropped receiver must signal stop");
    }

    #[tokio::test]
    async fn send_listed_entries_boundary_stops_when_receiver_dropped() {
        // max_depth=1 with a deeper key synthesizes a CommonPrefix at the
        // boundary; the dropped receiver makes that send fail, signalling stop.
        let engine = make_engine_with_max_depth(
            MockPageFetcher::from_map(HashMap::new()),
            "bucket",
            Some("p/"),
            None,
            1,
            5,
            false,
            Some(1),
        );
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let seen = Mutex::new(HashSet::new());
        let stop = engine
            .send_listed_entries(vec![make_entry("p/a/b/c.txt")], &tx, Some(&seen))
            .await
            .unwrap();
        assert!(stop, "boundary CommonPrefix send failure must signal stop");
    }

    // =======================================================================
    // Sequential: receiver-dropped mid-page returns gracefully
    // =======================================================================

    #[tokio::test]
    async fn sequential_stops_when_receiver_dropped_on_objects() {
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/a.txt")],
                sub_prefixes: vec!["p/sub/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            Some("/"),
            1,
            5,
            false,
        );
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let r = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        assert!(r.is_ok(), "dropped receiver is graceful: {r:?}");
    }

    #[tokio::test]
    async fn sequential_stops_when_receiver_dropped_on_prefixes() {
        // No objects, only sub-prefixes: the object send is a no-op and the
        // CommonPrefix send fails on the dropped receiver.
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/sub/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            Some("/"),
            1,
            5,
            false,
        );
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let r = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        assert!(r.is_ok(), "dropped receiver is graceful: {r:?}");
    }

    // =======================================================================
    // Version-listing pagination guards (sequential)
    // =======================================================================

    async fn drain(rx: &mut tokio::sync::mpsc::Receiver<ListEntry>) {
        while rx.recv().await.is_some() {}
    }

    #[tokio::test]
    async fn sequential_versions_truncated_without_marker_errors() {
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec![],
                is_truncated: true,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            Some("/"),
            1,
            5,
            false,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let r = engine.list_dispatch(ListingMode::Versions, &tx, 1000).await;
        drop(tx);
        drain(&mut rx).await;
        let err = r.unwrap_err();
        assert!(err.to_string().contains("no next key marker"), "got: {err}");
    }

    #[tokio::test]
    async fn sequential_versions_repeated_marker_errors() {
        let page = || ListPage {
            objects: vec![],
            sub_prefixes: vec![],
            is_truncated: true,
            continuation_token: None,
            key_marker: Some("m1".to_string()),
            version_id_marker: Some("v1".to_string()),
        };
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![page(), page()],
        );
        let engine = make_engine(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            Some("/"),
            1,
            5,
            false,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let r = engine.list_dispatch(ListingMode::Versions, &tx, 1000).await;
        drop(tx);
        drain(&mut rx).await;
        let err = r.unwrap_err();
        assert!(
            err.to_string().contains("same key/version marker twice"),
            "got: {err}"
        );
    }

    // =======================================================================
    // Pagination guards (parallel discovery loop)
    // =======================================================================

    fn truncated_objects_no_token() -> PageMap {
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec![],
                is_truncated: true,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        map
    }

    #[tokio::test]
    async fn parallel_objects_truncated_without_token_errors() {
        let engine = make_engine(
            MockPageFetcher::from_map(truncated_objects_no_token()),
            "bucket",
            Some("p/"),
            None, // no delimiter -> parallel path
            4,
            5,
            false,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let r = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        drop(tx);
        drain(&mut rx).await;
        let err = r.unwrap_err();
        assert!(
            err.to_string().contains("no continuation token"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn parallel_objects_repeated_token_errors() {
        let page = || ListPage {
            objects: vec![],
            sub_prefixes: vec![],
            is_truncated: true,
            continuation_token: Some("tok".to_string()),
            key_marker: None,
            version_id_marker: None,
        };
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![page(), page()],
        );
        let engine = make_engine(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            None,
            4,
            5,
            false,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let r = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        drop(tx);
        drain(&mut rx).await;
        let err = r.unwrap_err();
        assert!(
            err.to_string().contains("same continuation token twice"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn parallel_versions_truncated_without_marker_errors() {
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec![],
                is_truncated: true,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            None,
            4,
            5,
            false,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let r = engine.list_dispatch(ListingMode::Versions, &tx, 1000).await;
        drop(tx);
        drain(&mut rx).await;
        let err = r.unwrap_err();
        assert!(err.to_string().contains("no next key marker"), "got: {err}");
    }

    #[tokio::test]
    async fn parallel_versions_repeated_marker_errors() {
        let page = || ListPage {
            objects: vec![],
            sub_prefixes: vec![],
            is_truncated: true,
            continuation_token: None,
            key_marker: Some("m1".to_string()),
            version_id_marker: Some("v1".to_string()),
        };
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![page(), page()],
        );
        let engine = make_engine(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            None,
            4,
            5,
            false,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let r = engine.list_dispatch(ListingMode::Versions, &tx, 1000).await;
        drop(tx);
        drain(&mut rx).await;
        let err = r.unwrap_err();
        assert!(
            err.to_string().contains("same key/version marker twice"),
            "got: {err}"
        );
    }

    // =======================================================================
    // Parallel: pre-cancellation, send-cancel, boundary emit, sub-task panic
    // =======================================================================

    #[tokio::test]
    async fn parallel_returns_immediately_when_pre_cancelled() {
        let token = create_pipeline_cancellation_token();
        token.cancel();
        let engine = make_engine_with_token(
            MockPageFetcher::from_map(HashMap::new()),
            "bucket",
            Some("p/"),
            None,
            4,
            5,
            false,
            token,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let r = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        drop(tx);
        let mut n = 0;
        while rx.recv().await.is_some() {
            n += 1;
        }
        assert!(r.is_ok());
        assert_eq!(n, 0, "cancelled before fetching: no entries");
    }

    #[tokio::test]
    async fn parallel_stops_when_receiver_dropped_on_objects() {
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/a.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            None,
            4,
            5,
            false,
        );
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let r = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        assert!(r.is_ok(), "dropped receiver is graceful: {r:?}");
    }

    #[tokio::test]
    async fn parallel_emits_boundary_prefixes_at_max_depth() {
        // content max_depth = 1: the depth-0 discovery emits its sub-prefixes
        // as CommonPrefix entries instead of recursing into them.
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/a/".to_string(), "p/b/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine_with_max_depth(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            None,
            4,
            5,
            false,
            Some(1),
        );
        let entries = collect_entries(&engine, ListingMode::Objects, 1000)
            .await
            .unwrap();
        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["p/a/".to_string(), "p/b/".to_string()]);
        assert!(
            entries
                .iter()
                .all(|e| matches!(e, ListEntry::CommonPrefix(_))),
            "boundary sub-prefixes must be emitted as CommonPrefix"
        );
    }

    /// Fetcher that panics for one sub-prefix, to exercise sub-task join errors.
    #[derive(Clone)]
    struct PanicFetcher {
        panic_prefix: String,
        fallback: MockPageFetcher,
    }

    #[async_trait]
    impl PageFetcher for PanicFetcher {
        async fn fetch_page(
            &self,
            mode: ListingMode,
            max_keys: i32,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            continuation_token: Option<&str>,
            key_marker: Option<&str>,
            version_id_marker: Option<&str>,
            start_after: Option<&str>,
        ) -> Result<ListPage> {
            if prefix == Some(self.panic_prefix.as_str()) {
                panic!("fetch boom");
            }
            self.fallback
                .fetch_page(
                    mode,
                    max_keys,
                    prefix,
                    delimiter,
                    continuation_token,
                    key_marker,
                    version_id_marker,
                    start_after,
                )
                .await
        }
    }

    #[tokio::test]
    async fn parallel_sub_task_panic_is_reported() {
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/boom/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let fetcher = PanicFetcher {
            panic_prefix: "p/boom/".to_string(),
            fallback: MockPageFetcher::from_map(map),
        };
        let engine = make_engine(fetcher, "bucket", Some("p/"), None, 4, 5, false);
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let r = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        drop(tx);
        drain(&mut rx).await;
        let err = r.unwrap_err();
        assert!(
            err.to_string().contains("Listing sub-task panicked"),
            "got: {err}"
        );
    }

    #[test]
    fn prefix_at_depth_returns_none_when_key_lacks_enough_slashes() {
        let engine = make_engine(
            MockPageFetcher::from_map(HashMap::new()),
            "bucket",
            Some("p/"),
            None,
            1,
            5,
            false,
        );
        // relative part "nofile" has no slash, so the 2nd-slash boundary
        // doesn't exist and prefix_at_depth returns None.
        assert_eq!(engine.prefix_at_depth("p/nofile", 2), None);
        // A key not under the prefix also yields None.
        assert_eq!(engine.prefix_at_depth("other/x", 1), None);
    }

    #[tokio::test]
    async fn parallel_boundary_emit_stops_when_receiver_dropped() {
        // Boundary emit at max_depth sends CommonPrefix entries directly; a
        // dropped receiver makes that send fail and the task returns gracefully.
        let mut map: PageMap = HashMap::new();
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/a/".to_string(), "p/b/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine_with_max_depth(
            MockPageFetcher::from_map(map),
            "bucket",
            Some("p/"),
            None,
            4,
            5,
            false,
            Some(1),
        );
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let r = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        assert!(r.is_ok(), "dropped receiver is graceful: {r:?}");
    }

    // =======================================================================
    // convert_object / convert_object_version: restore-status mapping
    // =======================================================================

    #[test]
    fn convert_object_maps_restore_status() {
        use aws_sdk_s3::types::{Object, RestoreStatus};
        use aws_smithy_types::DateTime as SmithyDateTime;

        let obj = Object::builder()
            .key("k.txt")
            .size(10)
            .last_modified(SmithyDateTime::from_secs(1_700_000_000))
            .restore_status(
                RestoreStatus::builder()
                    .is_restore_in_progress(false)
                    .restore_expiry_date(SmithyDateTime::from_secs(1_700_100_000))
                    .build(),
            )
            .build();

        match convert_object(&obj).expect("object should convert") {
            ListEntry::Object(o) => {
                assert_eq!(o.is_restore_in_progress, Some(false));
                assert!(
                    o.restore_expiry_date.is_some(),
                    "restore expiry should be mapped"
                );
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn convert_object_version_maps_restore_status() {
        use aws_sdk_s3::types::{ObjectVersion, RestoreStatus};
        use aws_smithy_types::DateTime as SmithyDateTime;

        let version = ObjectVersion::builder()
            .key("k.txt")
            .version_id("v1")
            .is_latest(true)
            .size(20)
            .last_modified(SmithyDateTime::from_secs(1_700_000_000))
            .restore_status(
                RestoreStatus::builder()
                    .is_restore_in_progress(true)
                    .build(),
            )
            .build();

        match convert_object_version(&version).expect("version should convert") {
            ListEntry::Object(o) => {
                assert_eq!(o.is_restore_in_progress, Some(true));
                assert_eq!(o.version_id(), Some("v1"));
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Key-range splitting
    // -----------------------------------------------------------------------

    /// How an [`InMemoryFetcher`] treats `start-after`.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum StartAfterSupport {
        /// Compliant endpoint.
        Honor,
        /// An endpoint that ignores `start-after` on page requests but
        /// honours it for `max-keys=1` probes: splits get planned, and the
        /// range tasks must notice that their lower bound was ignored.
        IgnoreOnPages,
    }

    /// Simulates ListObjectsV2 / ListObjectVersions over a sorted key set:
    /// prefix filtering, "/" delimiter roll-up, `max-keys` pagination with
    /// continuation tokens / markers, and `start-after` / `key-marker`.
    /// Every key has exactly one version.
    #[derive(Clone)]
    struct InMemoryFetcher {
        keys: Arc<Vec<String>>,
        start_after: StartAfterSupport,
        calls: Arc<AtomicU64>,
    }

    impl InMemoryFetcher {
        fn new(mut keys: Vec<String>) -> Self {
            keys.sort();
            keys.dedup();
            Self {
                keys: Arc::new(keys),
                start_after: StartAfterSupport::Honor,
                calls: Arc::new(AtomicU64::new(0)),
            }
        }

        fn with_start_after(mut self, support: StartAfterSupport) -> Self {
            self.start_after = support;
            self
        }
    }

    fn make_version_entry(key: &str) -> ListEntry {
        match make_entry(key) {
            ListEntry::Object(mut obj) => {
                obj.version_info = Some(VersionInfo {
                    version_id: "v1".to_string(),
                    is_latest: true,
                });
                ListEntry::Object(obj)
            }
            other => other,
        }
    }

    #[async_trait]
    impl PageFetcher for InMemoryFetcher {
        async fn fetch_page(
            &self,
            mode: ListingMode,
            max_keys: i32,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            continuation_token: Option<&str>,
            key_marker: Option<&str>,
            _version_id_marker: Option<&str>,
            start_after: Option<&str>,
        ) -> Result<ListPage> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let prefix = prefix.unwrap_or("");
            let start_after = match self.start_after {
                StartAfterSupport::Honor => start_after,
                StartAfterSupport::IgnoreOnPages if max_keys == 1 => start_after,
                StartAfterSupport::IgnoreOnPages => None,
            };
            let after = match mode {
                ListingMode::Objects => continuation_token.or(start_after),
                ListingMode::Versions => key_marker,
            };
            let first = after.map_or(0, |a| self.keys.partition_point(|k| k.as_str() <= a));
            let max_keys = usize::try_from(max_keys).unwrap();

            let mut objects = Vec::new();
            let mut sub_prefixes: Vec<String> = Vec::new();
            let mut last_consumed: Option<&str> = None;
            let mut truncated = false;
            for key in &self.keys[first..] {
                let Some(rel) = key.strip_prefix(prefix) else {
                    if key.as_str() > prefix {
                        break;
                    }
                    continue;
                };
                let entries = objects.len() + sub_prefixes.len();
                match delimiter.and_then(|d| rel.find(d).map(|i| i + d.len())) {
                    Some(end) => {
                        let cp = format!("{prefix}{}", &rel[..end]);
                        if sub_prefixes.last() == Some(&cp) {
                            last_consumed = Some(key);
                            continue;
                        }
                        if entries == max_keys {
                            truncated = true;
                            break;
                        }
                        sub_prefixes.push(cp);
                    }
                    None => {
                        if entries == max_keys {
                            truncated = true;
                            break;
                        }
                        objects.push(match mode {
                            ListingMode::Objects => make_entry(key),
                            ListingMode::Versions => make_version_entry(key),
                        });
                    }
                }
                last_consumed = Some(key);
            }
            let token = truncated
                .then(|| last_consumed.map(str::to_string))
                .flatten();
            Ok(match mode {
                ListingMode::Objects => ListPage {
                    objects,
                    sub_prefixes,
                    is_truncated: truncated,
                    continuation_token: token,
                    key_marker: None,
                    version_id_marker: None,
                },
                ListingMode::Versions => ListPage {
                    objects,
                    sub_prefixes,
                    is_truncated: truncated,
                    continuation_token: None,
                    version_id_marker: token.as_ref().map(|_| "v1".to_string()),
                    key_marker: token,
                },
            })
        }
    }

    fn range_engine(
        fetcher: InMemoryFetcher,
        prefix: Option<&str>,
        max_parallel: u16,
        parallel_depth: u16,
        threshold: u32,
        content_max_depth: Option<u16>,
    ) -> ListingEngine<InMemoryFetcher> {
        let mut engine = make_engine_with_max_depth(
            fetcher,
            "bucket",
            prefix,
            None,
            max_parallel,
            parallel_depth,
            false,
            content_max_depth,
        );
        engine.parallel_range_split_threshold = threshold;
        engine
    }

    /// Like `collect_entries`, but drains the channel concurrently so large
    /// listings cannot block on the channel capacity. Returns sorted object
    /// keys and sorted common prefixes, failing on any duplicate.
    async fn collect_sorted(
        engine: &ListingEngine<InMemoryFetcher>,
        mode: ListingMode,
        max_keys: i32,
    ) -> (Vec<String>, Vec<String>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let drain = tokio::spawn(async move {
            let mut entries = Vec::new();
            while let Some(e) = rx.recv().await {
                entries.push(e);
            }
            entries
        });
        engine.list_dispatch(mode, &tx, max_keys).await.unwrap();
        drop(tx);
        let entries = drain.await.unwrap();

        let mut objects = Vec::new();
        let mut prefixes = Vec::new();
        for entry in entries {
            match entry {
                ListEntry::CommonPrefix(p) => prefixes.push(p),
                other => objects.push(other.key().to_string()),
            }
        }
        for list in [&mut objects, &mut prefixes] {
            list.sort();
            let before = list.len();
            list.dedup();
            assert_eq!(before, list.len(), "duplicate entries emitted");
        }
        (objects, prefixes)
    }

    fn sorted(mut keys: Vec<String>) -> Vec<String> {
        keys.sort();
        keys.dedup();
        keys
    }

    #[tokio::test]
    async fn range_split_lists_flat_bucket_completely() {
        let keys: Vec<String> = (0..12_000).map(|i| format!("file{i:05}.dat")).collect();
        let engine = range_engine(InMemoryFetcher::new(keys.clone()), None, 8, 2, 1000, None);

        let (objects, prefixes) = collect_sorted(&engine, ListingMode::Objects, 1000).await;

        assert_eq!(objects, keys);
        assert!(prefixes.is_empty());
        assert!(
            engine.range_split_counter.load(Ordering::Relaxed) >= 1,
            "a flat 12k-key bucket must be range-split"
        );
    }

    #[tokio::test]
    async fn range_split_handles_unicode_keys() {
        let scripts = ["東京", "大阪", "名古屋", "Москва", "😀", "abc", "ÄÖÜ", "z"];
        let keys: Vec<String> = (0..4000)
            .map(|i| format!("{}{:04}", scripts[i % scripts.len()], i))
            .collect();
        let engine = range_engine(InMemoryFetcher::new(keys.clone()), None, 8, 2, 200, None);

        let (objects, _) = collect_sorted(&engine, ListingMode::Objects, 100).await;

        assert_eq!(objects, sorted(keys));
        assert!(engine.range_split_counter.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn range_split_in_discovery_mode_rediscovers_sub_prefixes_once() {
        // Sub-prefixes interleave with keys on every page, so the cursor of a
        // split sits below some already-seen prefixes: those must be dropped
        // by the parent and found again by exactly one range.
        let mut keys: Vec<String> = Vec::new();
        for i in 0..3000 {
            keys.push(format!("k{i:04}"));
            if i % 3 == 0 {
                keys.push(format!("k{i:04}d/inner"));
            }
        }
        let engine = range_engine(InMemoryFetcher::new(keys.clone()), None, 8, 2, 300, None);

        let (objects, prefixes) = collect_sorted(&engine, ListingMode::Objects, 100).await;

        assert_eq!(objects, sorted(keys));
        assert!(prefixes.is_empty());
        assert!(engine.range_split_counter.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn range_split_emits_max_depth_boundary_prefix_once() {
        // Leaf "dNN/" lists without a delimiter; keys at depth 3 collapse to
        // boundary prefixes "dNN/xMM/" which straddle range boundaries.
        let mut keys: Vec<String> = Vec::new();
        for d in 0..12 {
            for x in 0..60 {
                for y in 0..3 {
                    keys.push(format!("d{d:02}/x{x:02}/y{y}"));
                }
            }
        }
        let engine = range_engine(InMemoryFetcher::new(keys), None, 8, 0, 50, Some(2));

        let (objects, prefixes) = collect_sorted(&engine, ListingMode::Objects, 20).await;

        let expected: Vec<String> = (0..12)
            .flat_map(|d| (0..60).map(move |x| format!("d{d:02}/x{x:02}/")))
            .collect();
        assert!(objects.is_empty());
        assert_eq!(prefixes, sorted(expected));
        assert!(engine.range_split_counter.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn range_split_disabled_when_threshold_is_zero() {
        let keys: Vec<String> = (0..3000).map(|i| format!("file{i:05}")).collect();
        let fetcher = InMemoryFetcher::new(keys.clone());
        let engine = range_engine(fetcher.clone(), None, 8, 2, 0, None);

        let (objects, _) = collect_sorted(&engine, ListingMode::Objects, 1000).await;

        assert_eq!(objects, keys);
        assert_eq!(engine.range_split_counter.load(Ordering::Relaxed), 0);
        assert_eq!(
            fetcher.calls.load(Ordering::Relaxed),
            3,
            "plain pagination only"
        );
    }

    #[tokio::test]
    async fn range_split_not_used_for_express_one_zone() {
        let keys: Vec<String> = (0..3000).map(|i| format!("file{i:05}")).collect();
        let mut engine = range_engine(InMemoryFetcher::new(keys.clone()), None, 8, 2, 1000, None);
        engine.bucket = "bucket--usw2-az1--x-s3".to_string();
        engine.allow_parallel_listings_in_express_one_zone = true;

        let (objects, _) = collect_sorted(&engine, ListingMode::Objects, 1000).await;

        assert_eq!(objects, keys);
        assert_eq!(engine.range_split_counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn range_split_falls_back_when_endpoint_ignores_start_after() {
        let keys: Vec<String> = (0..6000).map(|i| format!("file{i:05}")).collect();
        let fetcher =
            InMemoryFetcher::new(keys.clone()).with_start_after(StartAfterSupport::IgnoreOnPages);
        let engine = range_engine(fetcher, None, 8, 2, 1000, None);

        let (objects, _) = collect_sorted(&engine, ListingMode::Objects, 1000).await;

        assert_eq!(objects, keys, "listing stays complete and duplicate-free");
        assert!(engine.range_split_unsupported.load(Ordering::Relaxed));
        assert_eq!(
            engine.range_split_counter.load(Ordering::Relaxed),
            1,
            "no further splits once the endpoint is known to ignore start-after"
        );
    }

    #[tokio::test]
    async fn range_split_lists_versions() {
        let keys: Vec<String> = (0..4000).map(|i| format!("file{i:05}")).collect();
        let engine = range_engine(InMemoryFetcher::new(keys.clone()), None, 8, 2, 1000, None);

        let (objects, _) = collect_sorted(&engine, ListingMode::Versions, 1000).await;

        assert_eq!(objects, keys);
        assert!(engine.range_split_counter.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn range_split_parallelizes_skewed_tree_leaf() {
        let mut keys: Vec<String> = (0..6000).map(|i| format!("a/big/{i:05}")).collect();
        keys.extend((0..10).map(|i| format!("b/small/{i:02}")));
        let engine = range_engine(InMemoryFetcher::new(keys.clone()), None, 8, 1, 1000, None);

        let (objects, _) = collect_sorted(&engine, ListingMode::Objects, 1000).await;

        assert_eq!(objects, sorted(keys));
        assert!(
            engine.range_split_counter.load(Ordering::Relaxed) >= 1,
            "the oversized leaf must be range-split"
        );
    }

    #[tokio::test]
    async fn range_split_respects_engine_prefix() {
        let mut keys: Vec<String> = (0..3000).map(|i| format!("p/file{i:05}")).collect();
        keys.extend((0..3000).map(|i| format!("q/file{i:05}")));
        keys.push("p".to_string());
        keys.push("p0".to_string());
        let engine = range_engine(
            InMemoryFetcher::new(keys.clone()),
            Some("p/"),
            8,
            2,
            500,
            None,
        );

        let (objects, _) = collect_sorted(&engine, ListingMode::Objects, 250).await;

        let expected: Vec<String> = keys.into_iter().filter(|k| k.starts_with("p/")).collect();
        assert_eq!(objects, sorted(expected));
        assert!(engine.range_split_counter.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn next_char_skips_surrogates_and_ends_at_max() {
        assert_eq!(next_char('a'), Some('b'));
        assert_eq!(next_char('\u{D7FF}'), Some('\u{E000}'));
        assert_eq!(next_char(char::MAX), None);
    }

    #[test]
    fn common_prefix_chars_is_char_aligned() {
        assert_eq!(
            common_prefix_chars(&["file001", "file002"]),
            "file00".chars().collect::<Vec<_>>()
        );
        assert_eq!(common_prefix_chars(&["東京1", "東京2", "東大"]), vec!['東']);
        assert_eq!(common_prefix_chars(&["a"]), vec!['a']);
        assert!(common_prefix_chars(&["a", "b"]).is_empty());
        assert!(common_prefix_chars(&[]).is_empty());
    }

    #[test]
    fn candidate_boundary_chars_follow_observed_alphabet() {
        let hex: BTreeSet<char> = "0123456789abcdef".chars().collect();
        let chars = candidate_boundary_chars(Some('3'), &hex, false);
        assert_eq!(chars, "456789abcdef".chars().collect::<Vec<_>>());

        // Alphabet exhausted: the successor of `after` is still probed.
        assert_eq!(candidate_boundary_chars(Some('f'), &hex, false), vec!['g']);
        // No observed character at all: only the successor.
        assert_eq!(
            candidate_boundary_chars(Some('東'), &BTreeSet::new(), false),
            vec![next_char('東').unwrap()]
        );
        // Nothing known: alphabet only.
        assert_eq!(candidate_boundary_chars(None, &hex, false).len(), 16);

        let with_slash: BTreeSet<char> = "./0".chars().collect();
        assert_eq!(
            candidate_boundary_chars(Some('.'), &with_slash, true),
            vec!['0']
        );
        assert_eq!(
            candidate_boundary_chars(Some('.'), &with_slash, false),
            vec!['/', '0']
        );

        let big: BTreeSet<char> = (0x4E00..0x4E00 + 500).filter_map(char::from_u32).collect();
        let sampled = candidate_boundary_chars(None, &big, false);
        assert_eq!(sampled.len(), MAX_BOUNDARY_PROBES);
        assert!(sampled.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn key_range_bounds() {
        let range = KeyRange {
            start_after: Some("b".to_string()),
            version_id_marker: None,
            end_inclusive: Some("d".to_string()),
        };
        assert!(range.is_before_start("a"));
        assert!(range.is_before_start("b"));
        assert!(!range.is_before_start("c"));
        assert!(!range.is_beyond_end("d"));
        assert!(range.is_beyond_end("d0"));

        let mid_key = KeyRange {
            version_id_marker: Some("v".to_string()),
            ..range.clone()
        };
        assert!(
            !mid_key.is_before_start("b"),
            "resuming inside b's versions"
        );
        assert!(mid_key.is_before_start("a"));

        assert!(!KeyRange::default().is_before_start(""));
        assert!(!KeyRange::default().is_beyond_end("\u{10FFFF}"));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// Every key comes out exactly once regardless of key shape, page
        /// size, split threshold, worker count, or discovery depth.
        #[test]
        fn proptest_range_split_lists_every_key(
            keys in proptest::collection::vec("[ab/東😀-]{1,6}", 0..300),
            max_keys in 1i32..12,
            threshold in 1u32..40,
            max_parallel in 2u16..6,
            parallel_depth in 0u16..3,
            versions in any::<bool>(),
        ) {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let engine = range_engine(
                    InMemoryFetcher::new(keys.clone()),
                    None,
                    max_parallel,
                    parallel_depth,
                    threshold,
                    None,
                );
                let mode = if versions { ListingMode::Versions } else { ListingMode::Objects };
                let (objects, prefixes) = collect_sorted(&engine, mode, max_keys).await;
                prop_assert_eq!(objects, sorted(keys));
                prop_assert!(prefixes.is_empty());
                Ok(())
            })?;
        }
    }
}
