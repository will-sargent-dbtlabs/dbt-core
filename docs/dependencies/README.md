# Dependencies in dbt

## Process for adding a new dependency 
TBD

## Critical dependencies
TBD

## Dependencies per release
For each minor version, a requirements.txt will exist on the release branch that confirms a list of depedencies and their specific versions that are confirmed working for the given `dbt` version. The reason we are supplying this file is to allow consumers of `dbt` to pin dependency versions if desired to avoid breaking changes from underlying dependencies. We do not want to be perscriptive and pin all dependencies across the board for users since it could cause environment issues with conflicting dependencies. Instead, we will provide this file as an optional way to know what dependencies to pin if that is the desired way to consume `dbt`.
