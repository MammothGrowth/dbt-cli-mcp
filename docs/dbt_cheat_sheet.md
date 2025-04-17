### Primary dbt commands

These are the principal commands you will use most frequently with dbt. Not all of these will be available on dbt Cloud



* dbt development commands: dbt build
  * This command will load seeds, perform snapshots, run models, and execute tests
* dbt development commands: dbt compile
  * Generates executable SQL code of dbt models, analysis, and tests and outputs to the target folder
* dbt development commands: dbt docs
  * Generates and serves documentation for the dbt project (dbt docs generate, dbt docs serve)
* dbt development commands: dbt retry
  * Re-executes the last dbt command from the node point of failure. It references run_results.json to determine where to start
* dbt development commands: dbt run
  * Executes compiled SQL for the models in a dbt project against the target database
* dbt development commands: dbt run-operation
  * Is used to invoke a dbt macro from the command line. Typically used to run some arbitrary SQL against a database.
* dbt development commands: dbt seed
  * Loads CSV files located in the seeds folder into the target database
* dbt development commands: dbt show
  * Executes sql query against the target database and without materializing, displays the results to the terminal
* dbt development commands: dbt snapshot
  * Executes "snapshot" jobs defined in the snapshot folder of the dbt project
* dbt development commands: dbt source
  * Provides tools for working with source data to validate that sources are "fresh"
* dbt development commands: dbt test
  * Executes singular and generic tests defined on models, sources, snapshots, and seeds


### dbt Command arguments

The dbt commands above have options that allow you to select and exclude models as well as deferring to another environment like production instead of building dependent models for a given run. This table shows which options are available for each dbt command



* dbt command arguments: dbt build
  * --select / -s, --exclude, --selector, --resource-type, --defer, --empty, --full-refresh
* dbt command arguments: dbt compile
  * --select / -s, --exclude, --selector, --inline
* dbt command arguments: dbt docs generate
  *  --select / -s, --no-compile, --empty-catalog
* dbt command arguments: dbt docs serve
  *  --port
* dbt command arguments: dbt ls / dbt list
  * --select / -s, --exclude, --selector, --output, --output-keys, --resource-type, --verbose
* dbt command arguments: dbt run
  * --select / -s, --exclude, --selector, --resource-type, --defer, --empty, --full-refresh
* dbt command arguments: dbt seed
  * --select / -s, --exclude, --selector
* dbt command arguments: dbt show
  * --select / -s, --inline, --limit
* dbt command arguments: dbt snapshot
  * --select / -s, --exclude, --selector
* dbt command arguments: dbt source freshness
  * --select / -s, --exclude, --selector
* dbt command arguments: dbt source
  * --select / -s, --exclude, --selector, --output
* dbt command arguments: dbt test
  * --select / -s, --exclude, --selector, --defer


### dbt selectors

By combining the arguments above like "-s" with the options below, you can tell dbt which items you want to select or exclude. This can be a specific dbt model, everything in a specific folder, or now with the latest versions of dbt, the specific version of a model you are interested in.



* dbt node selectors: tag
  * Select models that match a specified tag
* dbt node selectors: source
  * Select models that select from a specified source
* dbt node selectors: path
  * Select models/sources defined at or under a specific path.
* dbt node selectors: file / fqn
  * Used to select a model by its filename, including the file extension (.sql).
* dbt node selectors: package
  * Select models defined within the root project or an installed dbt package.
* dbt node selectors: config
  * Select models that match a specified node config.
* dbt node selectors: test_type
  * Select tests based on their type, singular or generic, data, or unit (unit tests are available only in dbt 1.8)
* dbt node selectors: test_name
  * Select tests based on the name of the generic test that defines it.
* dbt node selectors: state
  * Select nodes by comparing them against a previous version of the same project, which is represented by a manifest. The file path of the comparison manifest must be specified via the --state flag or DBT_STATE environment variable.
* dbt node selectors: exposure
  * Select parent resources of a specified exposure.
* dbt node selectors: metric
  * Select parent resources of a specified metric.
* dbt node selectors: result
  * The result method is related to the state method described above and can be used to select resources based on their result status from a prior run.
* dbt node selectors: source_status
  * Select resource based on source freshness
* dbt node selectors: group
  * Select models defined within a group
* dbt node selectors: access
  * Selects models based on their access property.
* dbt node selectors: version
  * Selects versioned models based on their version identifier and latest version.


### dbt graph operators

dbt Graph Operator provide a powerful syntax that allow you to hone in on the specific items you want dbt to process.



* dbt graph operators: +
  * If "plus" (+) operator is placed at the front of the model selector, + will select all parents of the selected model. If placed at the end of the string, + will select all children of the selected model.
* dbt graph operators: n+
  * With the n-plus (n+) operator you can adjust the behavior of the + operator by quantifying the number of edges to step through.
* dbt graph operators: @
  * The "at" (@) operator is similar to +, but will also include the parents of the children of the selected model.
* dbt graph operators: *
  * The "star" (*) operator matches all models within a package or directory.


### Project level dbt commands

The following commands are used less frequently and perform actions like initializing a dbt project, installing dependencies, or validating that you can connect to your database.



* project level dbt commands: dbt clean
  * By default, this command deletes contents of the dbt_packages and target folders in the dbt project
* project level dbt commands: dbt clone
  * In databases that support it, can clone nodes (views/tables) to the current dbt target database, otherwise it creates a view pointing to the other environment
* project level dbt commands: dbt debug
  * Validates dbt project setup and tests connection to the database defined in profiles.yml
* project level dbt commands: dbt deps
  * Installs dbt package dependencies for the project as defined in packages.yml
* project level dbt commands: dbt init
  * Initializes a new dbt project and sets up the users's profiles.yml database connection
* project level dbt commands: dbt ls / dbt list
  * Lists resources defined in a dbt project such as modem, tests, and sources
* project level dbt commands: dbt parse
  * Parses and validates dbt files. It will fail if there are jinja and yaml errors in the project. It also outputs detailed timing info that may be useful when optimizing large projects
* project level dbt commands: dbt rpc
  * DEPRECATED after dbt 1.4. Runs an RPC server that compiles dbt models into SQL that can be submitted to a database by external tools


### dbt command line (CLI) flags

The flags below immediately follow the **dbt** command and go before the subcommand e.g. dbt _<FLAG>_ run

Read the official [dbt documentation](https://docs.getdbt.com/reference/global-configs/command-line-options)

‍



* dbt CLI flags (logging and debugging): -d, --debug / --no-debug
  * Display debug logging during dbt execution useful for debugging and making bug reports. Not to be confused with the dbt debug command which tests database connection.
* dbt CLI flags (logging and debugging): --log-cache-events / --no-log-cache-events
  * Enable verbose logging for relational cache events to help when debugging.
* dbt CLI flags (logging and debugging): --log-format [text|debug|json|default]
  * Specify the format of logging to the console and the log file.
* dbt CLI flags (logging and debugging): --log-format-file [text|debug|json|default]
  * Specify the format of logging to the log file by overriding the default format
* dbt CLI flags (logging and debugging): --log-level [debug|info|warn|error|none]
  * Specify the severity of events that are logged to the console and the log file.
* dbt CLI flags (logging and debugging): --log-level-file [debug|info|warn|error|none]
  * Specify the severity of events that are logged to the log file by overriding the default log level
* dbt CLI flags (logging and debugging): --log-path PATH
  * Configure the 'log-path'. Overrides 'DBT_LOG_PATH' if it is set.
* dbt CLI flags (logging and debugging): --print / --no-print
  * Outputs or hides all {{ print() }} statements within a macro call.
* dbt CLI flags (logging and debugging): --printer-width INTEGER
  * Sets the number of characters for terminal output
* dbt CLI flags (logging and debugging): -q, --quiet / --no-quiet
  * Suppress all non-error logging to stdout Does not affect {{ print() }} macro calls.
* dbt CLI flags (logging and debugging): --use-colors / --no-use-colors
  * Specify whether log output is colorized in the terminal
* dbt CLI flags (logging and debugging): --use-colors-file / --no-use-colors-file
  * Specify whether log file output is colorized


‍
