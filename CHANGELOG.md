# Change Log for ckanext-importer

The format of this file is based on [Keep a Changelog], and this
project uses [Semantic Versioning].

## [0.2.0] (2018-09-25)

### Changed

- The `on_error` parameter for `Importer.sync_package`,
  `Package.sync_resource`, and `Resource.sync_view` now defaults to
  `OnError.reraise`.

### Added

- Support for pagination during package search

### Fixed

- `Importer.sync_package` did not find existing private packages.

- Improved the handling of errors when entering the context managers and when
  uploading changes to CKAN.


## 0.1.0 (2018-09-12)

- First release


[Unreleased]: https://github.com/stadt-karlsruhe/ckanext-importer/compare/v0.2.0...master
[0.2.0]: https://github.com/stadt-karlsruhe/ckanext-importer/compare/v0.1.0...v0.2.0

[Keep a Changelog]: http://keepachangelog.com
[Semantic Versioning]: http://semver.org/

