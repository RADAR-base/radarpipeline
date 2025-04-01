"""
The command-line interface for the radarpipeline
"""
import argparse
from radarpipeline import radarpipeline
import warnings
warnings.filterwarnings("ignore")


def main():
    parser = argparse.ArgumentParser(
        description="A CLI interface for radarpipeline"
    )
    subparsers = parser.add_subparsers(dest="command", help="Sub-command help")

    # Subparser for 'run' command
    run_parser = subparsers.add_parser("run", help="Runs radarpipeline")
    run_parser.add_argument(
        "--config", "-f",
        help="Destination of config.yaml file"
    )
    run_parser.set_defaults(func=radarpipeline.run)

    # Subparser for 'validatååe' command
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate config file to run radarpipeline")

    validate_parser.add_argument(
        "--config", "-f",
        help="Destination of config.yaml file"
    )
    validate_parser.set_defaults(func=radarpipeline.validate)

    # Subparser for 'generate' command
    generate_parser = subparsers.add_parser(
        "generate",
        help="Generates a mock config file to run radarpipeline")

    generate_parser.add_argument(
        "--config", "-f",
        help="Destination of config.yaml file"
    )
    generate_parser.set_defaults(func=radarpipeline.generate_config)

    # Subparser for 'fetch' command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data using config file")
    fetch_parser.add_argument(
        "--config", "-f",
        help="Destination of config.yaml file"
    )
    fetch_parser.set_defaults(func=radarpipeline.fetch)

    # Subparser for 'convert' command
    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert radar data to custom format"
    )

    # Mutually exclusive group for source path or config file
    source_group = convert_parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--source_path", "-s",
        help="Path to the source data to be converted"
    )
    source_group.add_argument(
        "--config", "-f",
        help="Destination of config.yaml file"
    )

    # Additional arguments for conversion
    convert_parser.add_argument(
        "--dest_path", "-d",
        help="Path where the converted data will be saved",
        default="./"
    )
    convert_parser.add_argument(
        "--variables", "-v",
        help="Variables to be converted",
        required=True,
        nargs='+'
    )
    convert_parser.add_argument(
        "--dest_format", "-df",
        help="Format to convert the data into",
        default="csv"
    )

    convert_parser.set_defaults(func=radarpipeline.convert)
    # Subparser for 'list' command
    list_parser = subparsers.add_parser("list", help="List available Pipelines")
    list_parser.set_defaults(func=radarpipeline.show_available_pipelines)

    args = parser.parse_args()
    if 'config' in args:
        args.func(args.config)
    elif 'source_path' in args:
        args.func(source_path=args.source_path, destination_path=args.dest_path,
                  variables=args.variables, data_format=args.dest_format)
    elif args.command == "generate":
        args.func()
    elif args.command == "list":
        pipelines = radarpipeline.show_available_pipelines()
        # print the list of pipelines in a readable table format for terminal
        # Pipeliness is a list of dict with keys name, url and description
        print("{:<30} {:<60} {:<40}".format("Name", "URL", "Description"))
        print("=" * 130)
        for pipeline in pipelines:
            print("{:<30} | {:<60} | {:<40}".format(
                pipeline["name"], pipeline["url"], pipeline["description"]))
    elif args.command is None:
        parser.print_help()
    else:
        raise ValueError("Invalid arguments")


if __name__ == "__main__":
    main()
