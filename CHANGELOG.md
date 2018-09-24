# Change Log for ckanext-importer

The format of this file is based on [Keep a Changelog], and this
project uses [Semantic Versioning].

## [Unreleased]

### Changed

- The `on_error` parameter for `Importer.sync_package`, `Package.sync_resource`,
  and `Resource.sync_view` now defaults to `OnError.reraise`.

### Fixed

- `Importer.sync_package` did not find existing private packages.

- Improved the handling of errors during uploading of changes to CKAN.


## 0.1.0

- First release


[Unreleased]: https://github.com/stadt-karlsruhe/ckanext-importer/compare/v0.1.0...master

[Keep a Changelog]: http://keepachangelog.com
[Semantic Versioning]: http://semver.org/

