from radarpipeline import Project

def main():
    project = Project(input_data="config.yaml")
    project.fetch_data()
    print(project.data)


if __name__ == "__main__":
    main()