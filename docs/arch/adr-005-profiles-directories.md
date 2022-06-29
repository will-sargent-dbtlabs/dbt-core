# Search order for `profiles.yml`

## Context
dbt has a solid foundation of [convention over configuration](https://en.wikipedia.org/wiki/Convention_over_configuration) (CoC), and this proposal would lean into this further.

### Current behavior
dbt needs a `profiles.yml` configuration file for database connection info. I believe the current search order is:
1. `--profiles-dir` option
1. `DBT_PROFILES_DIR` environment variable
1. `~/.dbt/` directory

dbt also needs a `dbt_project.yml`. The current order of precedence is:
1. `--project-dir` option
1. current working directory

### Desired behavior
Search order for `profiles.yml`:
1. `--profiles-dir` option
1. `DBT_PROFILES_DIR` environment variable
1. current working directory (**NEW**)
1. `~/.dbt/` directory

Search order for `dbt_project.yml`:
1. `--project-dir` option
1. `DBT_PROJECT_DIR` environment variable (**NEW**)
1. current working directory




### General design requirements
There's two necessary pieces for dbt to use a profile to connect to a target database:
- property definitions for the desired target database
- secrets to plug into the definition slots

There are two main design requirements in terms of discoverability and accessibility:
1. property definitions are easily discoverable (since they are not sensitive)
1. secrets are non-discoverable and access is restricted per security policies

#### Approach 1

A reason given for the current order of precedence (emphasis mine):
> This file generally lives *outside of your dbt project* to avoid sensitive credentials being check in to version control. By default, dbt expects the `profiles.yml` file to be located in the `~/.dbt/` directory.

Using a `~/.dbt/profiles.yml` file is a solution that:
- can combine property definitions and secrets within a single file (but leaves optionality for them to be separated)
- is stored _outside_ of the project directory to guarantee that it is not tracked in version control (VCS)

##### Pros
- It can optionally utilize the same approach with environment variables as Solution 2 (below).
- Can theoretically re-use the same profile (including hard-coded secrets) across multiple projects (or multiple instances of a project)
##### Cons
- It is a little less obvious to see which environment variables need to be set -- need to actively search through the `profiles.yml` file

#### Approach 2
- store property definitions in `sample.profiles.yml` (or just `profiles.yml`)
- store secret definitions in:
    - `test.env.example` for local development
    - environment variables within the continuous integration (CI) environment
    - environment variables within non-CI environments (like production)

This requires doing all of the following for local development:
1. Copy the `sample.profiles.yml` file into `profiles.yml` (within the desired profiles directory)
1. Set `DBT_PROJECT_DIR` environment variable or `--profiles-dir` command-line interface (CLI) flag if profiles directory is different than `~/.dbt/`
1. Copy `test.env.example` file to `test.env`
1. Add secret values to `test.env`

##### Pros
- It is obvious to see which environment variables need to be set by looking at `test.env.example`

##### Cons
- Still need to configure the `profiles.yml` somehow (`~/.dbt/` or `DBT_PROJECT_DIR` or `--profiles-dir`)
    - There is _no way_ to clone a repo and have it "just work"





### Options

1. Default to current working directory for `profiles.yml`. Fall back to `~/.dbt/`.
1. Use existing `DBT_PROJECT_DIR` environment variable
1. Curated personal `~/.dbt/profiles.yml`
1. Put it in priority _behind_ `~/.dbt/`
1. Add a setting in `dbt_project.yml` (similar to `seed-paths`)
1. [`python-dotenv`](https://pypi.org/project/python-dotenv/)
1. [`direnv`](https://direnv.net/)
1. [Docker](https://docs.docker.com/get-docker/)

#### `DBT_PROJECT_DIR` environment variable

The most straight-forward solution to this currently is to just set the `DBT_PROJECT_DIR` environment variable to the root of the project (or some subdirectory).

##### Pros
- This functionality is already available and many people and systems use it
##### Cons
- Loading/unloading environment variables is not simple nor given knowledge.
- A problem is remembering to *unset* the `DBT_PROJECT_DIR` when switching to a different project that doesn't have profiles.yml in the current working directory.

#### Curated personal `~/.dbt/profiles.yml`
##### Pros
- definitely not checked into VCS on accident
- _can_ contain secrets
- doesn't _have_ to contain secrets -- can also utilize environment variables
##### Cons
- hard to read beyond a single project
- doesn't help for the case of continuous integration (CI) or production deployment
- undermines 12-factor app principles
    - twelve-factor apps store varying config in _environment variables_ (rather than configuration _files_) ([III. Config](https://12factor.net/config))
    - internal application config that does not vary between deploys is best done in the code (checked into version control)
    - the local `~/.dbt/profiles.yml` will surely be managed differently than the CI and production versions of the same file ([I. Codebase](https://12factor.net/codebase) and [X. Dev/prod parity](https://12factor.net/dev-prod-parity))

#### In priority behind `~/.dbt/`
##### Pros
- 100% certain to be non-breaking behavior
##### Cons
- Not actually the priority order we want
- Would require changing the behavior to the desired priority order upon the next major release.

#### Add a setting in `dbt_project.yml`
##### Pros
- 100% certain to be non-breaking behavior
- Allows the behavior to be configured on a per-project basis rather than a global basis
##### Cons
- Wouldn't be able to determine the final directory for `profiles.yml` until `dbt_project.yml` was found and parsed.
    - Maybe that's okay?

#### `python-dotenv`
##### Pros
- Works well for loading environment variables for testing and unloading when finished
- Can handle location of `profiles.yml` in the `DBT_PROJECT_DIR` environment variable
##### Cons
- Only works in the context of an invocation of the test suite
- Still requires manual file creation and editing

#### `direnv`
##### Pros
- `direnv` knows to unload variables when switching directories
- Has [installation instructions](https://direnv.net/docs/installation.html) across many platforms and shells
##### Cons
- Requires additional installation steps -- not possible for a "batteries-included" dbt project
- For security reasons, it requires running `direnv allow` the first time it is executed for a directory, and re-running it everytime the `.envrc` file is updated

#### Docker
##### Pros
- Docker containers can handle setting environment variables like `DBT_PROJECT_DIR`
- They are isolated from each other
##### Cons
- The end user needs to install Docker for their host operating system






## Decision
Default to current working directory for `profiles.yml`. Fall back to `~/.dbt/`.

## Status
Proposed

## Consequences

The instigating use-case:
- shipping a self-contained database + dbt project -- batteries-included!

Embedded databases like DuckDB and SQLite can utilize the same compute resources as `dbt-core` + `dbt-{adapter}`. In the case of non-sensitive data, `profiles.yml` defaulting to the current working directory would enable projects to work without mucking with environment variables.

This proposal would align conventions the order of search precedence for the `profiles.yml` and `dbt_project.yml` directories:
1. command line flag option
1. environment variable
1. one or more paths to search

### Pros
- Projects that have no secret values can work out of the box without any further intervention on the part of the user -- batteries included! 🔋
    - This can be especially useful for use cases like demo projects or projects exclusively containing public data sets.
- There are no additional tooling to install and manage (in contrast to most of the alternatives)
- Works great with existing conventions:
    - Merely don't create a `profiles.yml` in the current working directory if a centralized config in `~/.dbt/` is desired instead.

It also supports this exotic option:
- If `profiles.yml` _does_ need to contain plain-text secrets for some reason, you can still safely check it into version control using a tool like [BlackBox](https://github.com/StackExchange/blackbox) 🤯

### Cons
- This could be considered breaking behavior for any projects currently storing a `profiles.yml` in the project root.
    - However, this would only be breaking in the case that the local `profiles.yml` is non-functional / undesired (which feels unanticipated and unlikely)
    - Any local `profiles.yml` is most likely utilized anyways by the project via `DBT_PROJECT_DIR` or by copying into `~/.dbt/`
