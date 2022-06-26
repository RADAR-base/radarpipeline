from radarpipeline import Project


def main():
    project = Project(input_data="config.yaml")
    project.fetch_data()
    project.compute_features()


if __name__ == "__main__":
    main()
