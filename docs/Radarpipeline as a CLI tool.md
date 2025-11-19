# Radarpipeline as a CLI Tool

Radarpipeline provides a powerful command-line interface (CLI) that allows you to interact with the pipeline directly from your terminal. This document covers all available commands and their usage.

## Installation

To use the CLI, ensure radarpipeline is installed in your environment:

```bash
pip install radarpipeline
```

## Basic Usage

The CLI is accessed through the `radarpipeline` command followed by subcommands:

```bash
radarpipeline <command> [options]
```

To see all available commands:

```bash
radarpipeline --help
```

## Available Commands

### 1. `run` - Execute Pipeline

Runs the radarpipeline with a specified configuration file.

```bash
radarpipeline run --config path/to/config.yaml
# or
radarpipeline run -f path/to/config.yaml
```

**Options:**

- `--config`, `-f`: Path to the configuration YAML file (required)

**Example:**

```bash
radarpipeline run --config ./config.yaml
```

### 2. `validate` - Validate Configuration

Validates your configuration file without running the pipeline. This is useful for checking if your config file is properly formatted and contains all required parameters.

```bash
radarpipeline validate --config path/to/config.yaml
# or
radarpipeline validate -f path/to/config.yaml
```

**Options:**

- `--config`, `-f`: Path to the configuration YAML file to validate (required)

**Example:**

```bash
radarpipeline validate --config ./config.yaml
```

### 3. `generate` - Generate Configuration Template

Generates a mock configuration file that you can use as a starting point for your own pipeline configuration.

```bash
radarpipeline generate --config path/to/output/config.yaml
# or
radarpipeline generate -f path/to/output/config.yaml
```

**Options:**

- `--config`, `-f`: Destination path for the generated configuration file (optional)

**Example:**

```bash
radarpipeline generate --config ./my_config.yaml
```

### 4. `fetch` - Fetch Data

Fetches data using the parameters specified in your configuration file without processing it through the full pipeline.

```bash
radarpipeline fetch --config path/to/config.yaml
# or
radarpipeline fetch -f path/to/config.yaml
```

**Options:**

- `--config`, `-f`: Path to the configuration YAML file (required)

**Example:**

```bash
radarpipeline fetch --config ./config.yaml
```

### 5. `convert` - Convert Data Format

Converts radar data from one format to another. This command provides two ways to specify the source data:

#### Option A: Using source path directly

```bash
radarpipeline convert --source_path /path/to/source/data \
                     --dest_path /path/to/destination \
                     --variables variable1 variable2 variable3 \
                     --dest_format csv
```

#### Option B: Using configuration file

```bash
radarpipeline convert --config path/to/config.yaml \
                     --dest_path /path/to/destination \
                     --variables variable1 variable2 variable3 \
                     --dest_format csv
```

**Options:**

- `--source_path`, `-s`: Path to the source data to be converted (mutually exclusive with --config)
- `--config`, `-f`: Path to configuration file (mutually exclusive with --source_path)
- `--dest_path`, `-d`: Path where converted data will be saved (default: "./")
- `--variables`, `-v`: List of variables to be converted (required)
- `--dest_format`, `-df`: Output format for converted data (default: "csv")

**Examples:**

```bash
# Convert specific variables from a source path to CSV
radarpipeline convert -s ./raw_data -d ./converted_data -v heart_rate steps sleep_duration

# Convert using config file with custom format
radarpipeline convert -f ./config.yaml -d ./output -v accelerometer gyroscope -df parquet
```

### 6. `list` - List Available Pipelines

Displays all available pipeline configurations in a readable table format.

```bash
radarpipeline list
```

This command shows:

- Pipeline name
- URL/location
- Description

**Example output:**

```text
Name                           | URL                                                          | Description
================================================================================================
Basic Pipeline                 | https://github.com/RADAR-base/radarpipeline/basic           | Standard data processing pipeline
Advanced Analytics             | https://github.com/RADAR-base/radarpipeline/advanced        | Advanced analytics and ML features
Real-time Processing          | https://github.com/RADAR-base/radarpipeline/realtime        | Real-time data processing pipeline
```

## Common Usage Patterns

### 1. Quick Start Workflow

```bash
# Generate a configuration template
radarpipeline generate -f my_config.yaml

# Edit the configuration file with your parameters
# ... edit my_config.yaml ...

# Validate your configuration
radarpipeline validate -f my_config.yaml

# Run the pipeline
radarpipeline run -f my_config.yaml
```

### 2. Data Exploration Workflow

```bash
# List available pipelines
radarpipeline list

# Fetch data without processing
radarpipeline fetch -f config.yaml

# Convert data to different formats for analysis
radarpipeline convert -s ./data -d ./analysis -v heart_rate steps -df csv
```

### 3. Development and Testing

```bash
# Validate configuration during development
radarpipeline validate -f test_config.yaml

# Convert small datasets for testing
radarpipeline convert -s ./test_data -d ./test_output -v test_variable -df json
```

## Error Handling

The CLI provides helpful error messages for common issues:

- **Invalid configuration**: Use `validate` command to check your config file
- **Missing arguments**: The CLI will prompt you for required parameters
- **File not found**: Ensure all file paths are correct and accessible
- **Invalid format**: Check supported formats using the `list` command

## Tips and Best Practices

1. **Always validate** your configuration file before running the full pipeline
2. **Use absolute paths** when possible to avoid path-related issues
3. **Start small** by converting a subset of variables before processing large datasets
4. **Check available pipelines** regularly for new features and capabilities
5. **Use the fetch command** to test data connectivity before full processing

## Getting Help

For detailed help on any command, use:

```bash
radarpipeline <command> --help
```

For general help:

```bash
radarpipeline --help
```

## Integration with Other Tools

The CLI can be easily integrated into:

- **Shell scripts** for automated data processing
- **CI/CD pipelines** for continuous data validation
- **Cron jobs** for scheduled data processing
- **Docker containers** for containerized deployments
