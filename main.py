from radarpipeline import Project

project = Project(input_data="config.yaml")
project.fetch_data()