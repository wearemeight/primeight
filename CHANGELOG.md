# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Breaking change will be enphasized starting the line with **[BREAKING]** .

## [Unreleased]

## [0.1.6] - 2021-10-04
### Changed
- remove client_id verification
- add column time generators from uuid

## [0.1.5] - 2021-05-11

### Fixed
- fix bug where queries to materialized views failed with message "Missing keyspace" (Issue #7)
- fix bug where materialized views queries were being wrongly constructed (Issue #6)

## [0.1.4] - 2021-01-08

### Changed
- add flexibility to requirements file

## [0.1.3] - 2021-01-07

### Changed
- fix bug where map was not recognized as a collection by the parser
- fix bug where parser would crash if the `backup` field was not defined

## [0.1.2] - 2020-09-29

### Changed
- improve PyPi primeight page

## [0.1.1] - 2020-09-22

### Added
- initial version.
