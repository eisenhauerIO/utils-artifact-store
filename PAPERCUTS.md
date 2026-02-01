# Papercuts

Minor issues and potential improvements to revisit later.

## Jobs Module

- **Consider removing `generate_job_id()` and making `job_id` required in `create_job()`**
  - Rationale: Job ID generation should be the caller's responsibility, not the library's
  - The library shouldn't make assumptions about ID format preferences
  - Callers who need auto-generation can implement their own scheme
