"""
The command-line interface for the radarpipeline
"""
import argparse
from radarpipeline import radarpipeline


def main():
    parser = argparse.ArgumentParser(
        description="A cli interface for radarpipeline"
    )
    parser.add_argument(
        "run", type=str,
        help="Runs radarpipeline"
    )
    parser.add_argument(
        "--config", "-f",
        help=("Destination of config.yaml file")
    )
    
    args = parser.parse_args()


if __name__ == "__main__":
    main()
