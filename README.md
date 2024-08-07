# First Tee Decision Support Repository
This repository will store all code related to the decision support system effort.

## Local development
When getting ready to push a branch and merge to develop be sure to run ```cdk synth``` and ```pytest tests``` to check for errors or failing tests.

## Git Flow
Developers should also build features on a new branch then open a pull request to develop to merge and the CI/CD pipeline to run. Develop goes to UAT and finally UAT goes to main/prod. Each PR will kick off the pipeline for that environment of the same name.

## Development Team
|Team Member|Contact|
|---|---|
|Harry Schuster|hschuster@captechconsulting.com|


## Documentation
Most of the documentation accompanying the gamification effort can be found in the [First Tee Confluence environment here](https://firsttee.atlassian.net/wiki/spaces/DSS/overview?homepageId=475824403).